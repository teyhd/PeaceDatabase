using System.Collections.Generic;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using PeaceDatabase.Core.Models;
using Xunit;

namespace PeaceDatabase.Tests.Api
{
    public class WebApiCrudFlowTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly WebApplicationFactory<Program> _factory;

        public WebApiCrudFlowTests(WebApplicationFactory<Program> factory)
        {
            _factory = factory;
        }

        [Fact]
        public async void Full_Crud_And_Search_Flow()
        {
            var client = _factory.CreateClient();

            // 0) Create DB (idempotent)
            var putDb = await client.PutAsync("/v1/db/app", content: null);
            putDb.StatusCode.Should().Be(HttpStatusCode.OK);

            // 1) POST /docs
            var doc = new Document
            {
                Data = new Dictionary<string, object>
                {
                    ["type"] = "note",
                    ["rating"] = 8
                },
                Tags = new List<string> { "api", "green" },
                Content = "API document. Some text for fulltext search."
            };

            var createdResp = await client.PostAsJsonAsync("/v1/db/app/docs", doc);
            createdResp.StatusCode.Should().Be(HttpStatusCode.Created);
            var created = await createdResp.Content.ReadFromJsonAsync<Document>();
            created.Should().NotBeNull();
            var id = created!.Id;
            var rev1 = created.Rev;

            // 2) GET /docs/{id}
            var got = await client.GetFromJsonAsync<Document>($"/v1/db/app/docs/{id}");
            got.Should().NotBeNull();
            got!.Rev.Should().Be(rev1);

            // 3) _find/fields: equals + numeric range
            var findFieldsReq = new
            {
                EqualsMap = new Dictionary<string, string> { ["type"] = "note" },
                NumericField = "rating",
                Min = 5.0,
                Max = 10.0,
                Skip = 0,
                Limit = 10
            };

            var findFields = await client.PostAsJsonAsync("/v1/db/app/_find/fields", findFieldsReq);
            findFields.EnsureSuccessStatusCode();
            using var fieldsPayload = JsonDocument.Parse(await findFields.Content.ReadAsStringAsync());
            fieldsPayload.RootElement.GetProperty("total").GetInt32().Should().BeGreaterThan(0);

            // 4) _find/tags
            var findTagsReq = new
            {
                AllOf = new[] { "api" },
                AnyOf = new[] { "green", "blue" },
                NoneOf = new[] { "banned" },
                Skip = 0,
                Limit = 10
            };

            var findTags = await client.PostAsJsonAsync("/v1/db/app/_find/tags", findTagsReq);
            findTags.EnsureSuccessStatusCode();
            using var tagsPayload = JsonDocument.Parse(await findTags.Content.ReadAsStringAsync());
            tagsPayload.RootElement.GetProperty("total").GetInt32().Should().BeGreaterThan(0);

            // 5) _search?q=
            var search = await client.GetAsync("/v1/db/app/_search?q=fulltext%20document");
            search.EnsureSuccessStatusCode();
            using var searchPayload = JsonDocument.Parse(await search.Content.ReadAsStringAsync());
            searchPayload.RootElement.GetProperty("total").GetInt32().Should().BeGreaterThan(0);

            // 6) PUT /docs/{id} (update with correct rev)
            var updateDoc = new Document
            {
                Id = id,
                Rev = rev1,
                Data = new Dictionary<string, object>
                {
                    ["type"] = "note",
                    ["rating"] = 10
                },
                Tags = new List<string> { "api", "green", "updated" },
                Content = "API document updated. Now has extra words."
            };

            var put = await client.PutAsJsonAsync($"/v1/db/app/docs/{id}", updateDoc);
            put.StatusCode.Should().Be(HttpStatusCode.OK);
            var updated = await put.Content.ReadFromJsonAsync<Document>();
            updated.Should().NotBeNull();
            var rev2 = updated!.Rev;
            rev2.Should().NotBeNull().And.NotBe(rev1);

            // 7) DELETE /docs/{id}?rev=
            var del = await client.DeleteAsync($"/v1/db/app/docs/{id}?rev={rev2}");
            del.StatusCode.Should().Be(HttpStatusCode.OK);

            // 8) GET после удаления -> 404
            var afterDel = await client.GetAsync($"/v1/db/app/docs/{id}");
            afterDel.StatusCode.Should().Be(HttpStatusCode.NotFound);
        }
    }
}
