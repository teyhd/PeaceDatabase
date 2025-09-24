using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.InMemory;
using Xunit;

namespace PeaceDatabase.Tests.Storage
{
    public class InMemoryDocumentServiceCrudTests
    {
        private readonly IDocumentService _svc;

        public InMemoryDocumentServiceCrudTests()
        {
            _svc = new InMemoryDocumentService();
            _svc.CreateDb("docs");
        }

        [Fact]
        public void Post_Get_Update_Delete_Flow()
        {
            // 1) POST (auto-id)
            var post = _svc.Post("docs", new Document
            {
                Data = new Dictionary<string, object>
                {
                    ["type"] = "note",
                    ["rating"] = 5
                },
                Tags = new List<string> { "blue", "draft" },
                Content = "Hello world, this is the very first note."
            });

            post.Ok.Should().BeTrue();
            post.Doc.Should().NotBeNull();
            var id = post.Doc!.Id;
            var rev1 = post.Doc.Rev;
            id.Should().NotBeNullOrEmpty();
            rev1.Should().NotBeNullOrEmpty();

            // 2) GET
            var got1 = _svc.Get("docs", id);
            got1.Should().NotBeNull();
            got1!.Rev.Should().Be(rev1);
            //got1.Data!["rating"].Should().Be(5);
            ExtractInt(got1!.Data!["rating"]).Should().Be(5);

            // 3) PUT (update with proper _rev)
            var updated = new Document
            {
                Id = id,
                Rev = rev1,
                Data = new Dictionary<string, object>
                {
                    ["type"] = "note",
                    ["rating"] = 10
                },
                Tags = new List<string> { "blue", "published" },
                Content = "Hello world, this is an updated note."
            };
            var put = _svc.Put("docs", updated);
            put.Ok.Should().BeTrue();
            put.Doc.Should().NotBeNull();
            var rev2 = put.Doc!.Rev;
            rev2.Should().NotBeNullOrEmpty().And.NotBe(rev1);

            // 4) Конфликтная попытка (старый _rev)
            var conflict = _svc.Put("docs", new Document
            {
                Id = id,
                Rev = rev1, // старый
                Data = new Dictionary<string, object> { ["type"] = "note", ["rating"] = 11 },
                Tags = new List<string> { "blue" },
                Content = "conflicting update"
            });
            conflict.Ok.Should().BeFalse();
            conflict.Error.Should().NotBeNull();
            conflict.Error!.ToLowerInvariant().Should().Contain("conflict");

            // 5) FIND: equals + numeric range
            var eq = new Dictionary<string, string> { ["type"] = "note" };
            var range = (field: "rating", min: (double?)7, max: (double?)10);
            var f1 = _svc.FindByFields("docs", equals: eq, numericRange: range).ToList();
            f1.Should().ContainSingle(d => d.Id == id);
            ExtractInt(f1[0].Data!["rating"]).Should().Be(10);


            // 6) FIND: tags
            var f2 = _svc.FindByTags("docs", allOf: new[] { "blue" }, anyOf: new[] { "published" }).ToList();
            f2.Should().ContainSingle(d => d.Id == id);

            // 7) FULLTEXT
            var ff = _svc.FullTextSearch("docs", "updated note").ToList();
            ff.Should().ContainSingle(d => d.Id == id);

            // 8) DELETE (with current rev)
            var del = _svc.Delete("docs", id, rev2!);
            del.Ok.Should().BeTrue();

            // 9) GET after delete -> null (soft-delete скрывает в Get)
            var gotAfterDel = _svc.Get("docs", id);
            gotAfterDel.Should().BeNull();

            // 10) AllDocs includeDeleted: true вернёт удалённый
            var all = _svc.AllDocs("docs", includeDeleted: true).ToList();
            all.Any(d => d.Id == id).Should().BeTrue();

            // seq должен быть > 0
            _svc.Seq("docs").Should().BeGreaterThan(0);
        }
        private static int ExtractInt(object value)
        {
            return value switch
            {
                System.Text.Json.JsonElement je when je.ValueKind == System.Text.Json.JsonValueKind.Number
                    => je.GetInt32(),
                System.Text.Json.JsonElement je when je.ValueKind == System.Text.Json.JsonValueKind.String
                    => int.Parse(je.GetString()!),
                int i => i,
                long l => checked((int)l),
                string s => int.Parse(s),
                _ => throw new InvalidOperationException($"Unsupported numeric type: {value?.GetType().Name}")
            };
        }
    }
}
