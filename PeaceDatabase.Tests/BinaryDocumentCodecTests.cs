// File: PeaceDatabase.Tests/BinaryDocumentCodecTests.cs
using System;
using System.Collections.Generic;
using FluentAssertions;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Binary;
using Xunit;

namespace PeaceDatabase.Tests
{
    public class BinaryDocumentCodecTests
    {
        [Fact]
        public void Serialize_And_Deserialize_Document_AllFields()
        {
            var doc = new Document
            {
                Id = "doc-123",
                Rev = "1-abc",
                Deleted = false,
                Data = new Dictionary<string, object>
                {
                    ["name"] = "Alpha",
                    ["count"] = 42,
                    ["pi"] = 3.14,
                    ["flag"] = true,
                    ["tags"] = new List<string> { "a", "b" },
                    ["meta"] = new Dictionary<string, object> { ["k"] = "v" }
                },
                Tags = new List<string> { "x", "y" },
                Content = "Hello binary!"
            };

            var bytes = CustomBinaryDocumentCodec.Serialize(doc);
            bytes.Should().NotBeNullOrEmpty();

            var doc2 = CustomBinaryDocumentCodec.Deserialize(bytes);
            doc2.Should().NotBeNull();
            doc2.Id.Should().Be(doc.Id);
            doc2.Rev.Should().Be(doc.Rev);
            doc2.Deleted.Should().Be(doc.Deleted);
            doc2.Content.Should().Be(doc.Content);
            doc2.Tags.Should().BeEquivalentTo(doc.Tags);
            doc2.Data.Should().NotBeNull();
            doc2.Data!["name"].Should().Be("Alpha");
            doc2.Data!["count"].Should().Be(42);
            doc2.Data!["pi"].Should().BeOfType<double>().And.Be(3.14);
            doc2.Data!["flag"].Should().Be(true);
            doc2.Data!["tags"].Should().BeEquivalentTo(new List<string> { "a", "b" });
            var meta = doc2.Data!["meta"] as Dictionary<string, object>;
            meta.Should().NotBeNull();
            meta!["k"].Should().Be("v");
        }

        [Fact]
        public void Serialize_And_Deserialize_Document_Minimal()
        {
            var doc = new Document { Id = "id1", Deleted = true };
            var bytes = CustomBinaryDocumentCodec.Serialize(doc);
            var doc2 = CustomBinaryDocumentCodec.Deserialize(bytes);
            doc2.Id.Should().Be(doc.Id);
            doc2.Deleted.Should().BeTrue();
            doc2.Rev.Should().BeNull();
            doc2.Data.Should().BeNull();
            doc2.Tags.Should().BeNull();
            doc2.Content.Should().BeNull();
        }
    }
}
