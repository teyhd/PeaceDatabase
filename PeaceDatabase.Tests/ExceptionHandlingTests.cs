using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using Xunit;

namespace PeaceDatabase.Tests.Api;

public class ExceptionHandlingTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;

    public ExceptionHandlingTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task Invalid_json_returns_validation_problem()
    {
        using var client = _factory.CreateClient();
        await client.PutAsync("/v1/db/app", content: null);

        var response = await client.PostAsync(
            "/v1/db/app/docs",
            new StringContent("{\"id\":", Encoding.UTF8, "application/json"));

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        response.Content.Headers.ContentType?.MediaType.Should().Be("application/problem+json");

        var problem = await ParseProblem(response);
        problem.GetProperty("type").GetString().Should().Be("https://example.com/errors/validation");
        problem.GetProperty("title").GetString().Should().Be("Malformed JSON payload");
        problem.GetProperty("status").GetInt32().Should().Be(400);
        problem.GetProperty("traceId").GetString().Should().NotBeNullOrWhiteSpace();
        problem.GetProperty("errors").GetProperty("body").EnumerateArray().Should().ContainSingle();
    }


    [Fact]
    public async Task Model_validation_returns_problem_details()
    {
        var client = _factory.CreateClient();
        await client.PutAsync("/v1/db/app", content: null);

        using var content = JsonContent.Create<object?>(null);
        var response = await client.PostAsync("/v1/db/app/_find/fields", content);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var problem = await ParseProblem(response);
        problem.GetProperty("type").GetString().Should().Be("https://example.com/errors/validation");
        problem.GetProperty("title").GetString().Should().Be("Request validation failed");
        problem.GetProperty("errors").GetProperty("req").EnumerateArray().Should().NotBeEmpty();
    }

    [Fact]
    public async Task Not_found_exception_returns_problem_details()
    {
        var client = _factory.CreateClient();
        await client.PutAsync("/v1/db/app", content: null);

        var response = await client.GetAsync("/v1/db/app/docs/missing-id");

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var problem = await ParseProblem(response);
        problem.GetProperty("type").GetString().Should().Be("https://example.com/errors/not-found");
        problem.GetProperty("title").GetString().Should().Be("Resource not found");
    }

    [Fact]
    public async Task Conflict_exception_returns_problem_details()
    {
        using var client = _factory.CreateClient();
        var db = $"app-{Guid.NewGuid():N}";
        await client.PutAsync($"/v1/db/{db}", content: null);

        var id = $"conflict-{Guid.NewGuid():N}";
        var create = await client.PostAsJsonAsync($"/v1/db/{db}/docs", new Document
        {
            Id = id,
            Data = new Dictionary<string, object> { ["type"] = "note" }
        });
        create.StatusCode.Should().Be(HttpStatusCode.Created);

        var conflictPayload = new Document
        {
            Id = id,
            Rev = "1-notarev",
            Data = new Dictionary<string, object> { ["type"] = "note" }
        };

        var response = await client.PutAsJsonAsync($"/v1/db/{db}/docs/{id}", conflictPayload);

        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        var problem = await ParseProblem(response);
        problem.GetProperty("type").GetString().Should().Be("https://example.com/errors/conflict");
        problem.GetProperty("title").GetString().Should().Be("Operation conflict");
    }

    [Fact]
    public async Task Unexpected_exception_returns_internal_problem()
    {
        await using var factory = new ThrowingWebApplicationFactory();
        var client = factory.CreateClient();

        var response = await client.PutAsync("/v1/db/app", content: null);

        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        var problem = await ParseProblem(response);
        problem.GetProperty("type").GetString().Should().Be("https://example.com/errors/internal");
        problem.GetProperty("title").GetString().Should().Be("Unexpected server error");
        problem.TryGetProperty("detail", out var detail).Should().BeTrue();
        detail.ValueKind.Should().Be(JsonValueKind.Null);
        problem.GetProperty("traceId").GetString().Should().NotBeNullOrWhiteSpace();
    }

    private static async Task<JsonElement> ParseProblem(HttpResponseMessage response)
    {
        var payload = await response.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(payload);
        return json.RootElement.Clone(); // <-- вот это важно
    }

    private sealed class ThrowingWebApplicationFactory : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IDocumentService>();
                services.AddSingleton<IDocumentService, ThrowingDocumentService>();
            });
        }
    }

    private sealed class ThrowingDocumentService : IDocumentService
    {
        public (bool Ok, string? Error) CreateDb(string db) => throw new InvalidOperationException("boom");

        public (bool Ok, string? Error) DeleteDb(string db) => throw new InvalidOperationException("boom");

        public Document? Get(string db, string id, string? rev = null) => throw new InvalidOperationException("boom");

        public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc) => throw new InvalidOperationException("boom");

        public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc) => throw new InvalidOperationException("boom");

        public (bool Ok, string? Error) Delete(string db, string id, string rev) => throw new InvalidOperationException("boom");

        public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true) => throw new InvalidOperationException("boom");

        public int Seq(string db) => throw new InvalidOperationException("boom");

        public IEnumerable<Document> FindByFields(string db, IDictionary<string, string>? equals = null, (string field, double? min, double? max)? numericRange = null, int skip = 0, int limit = 100) => throw new InvalidOperationException("boom");

        public IEnumerable<Document> FindByTags(string db, IEnumerable<string>? allOf = null, IEnumerable<string>? anyOf = null, IEnumerable<string>? noneOf = null, int skip = 0, int limit = 100) => throw new InvalidOperationException("boom");

        public IEnumerable<Document> FullTextSearch(string db, string query, int skip = 0, int limit = 100) => throw new InvalidOperationException("boom");
    }
}
