// File: PeaceDatabase.Tests/BinaryDocumentCodecNestedTests.cs
using System;
using System.Collections.Generic;
using FluentAssertions;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Binary;
using Xunit;

namespace PeaceDatabase.Tests
{
    public class BinaryDocumentCodecNestedTests
    {
        [Fact]
        public void Serialize_And_Deserialize_Document_With_Nested_Arrays()
        {
            // Create a document similar to AMZN structure with nested arrays
            var doc = new Document
            {
                Id = "amzn-test",
                Data = new Dictionary<string, object>
                {
                    ["chart"] = new Dictionary<string, object>
                    {
                        ["result"] = new List<object>
                        {
                            new Dictionary<string, object>
                            {
                                ["meta"] = new Dictionary<string, object>
                                {
                                    ["currency"] = "USD",
                                    ["symbol"] = "AMZN",
                                    ["price"] = 88.505
                                },
                                ["timestamp"] = new List<object> { 863703000, 863789400, 864048600 },
                                ["indicators"] = new List<object>
                                {
                                    new Dictionary<string, object> { ["close"] = new List<object> { 1.0, 2.0, 3.0 } },
                                    new Dictionary<string, object> { ["volume"] = new List<object> { 100, 200, 300 } }
                                }
                            }
                        }
                    }
                }
            };

            var bytes = CustomBinaryDocumentCodec.Serialize(doc);
            bytes.Should().NotBeNullOrEmpty();

            var doc2 = CustomBinaryDocumentCodec.Deserialize(bytes);
            doc2.Should().NotBeNull();
            doc2.Id.Should().Be(doc.Id);
            doc2.Data.Should().NotBeNull();

            // Verify nested structure is preserved
            var chart = doc2.Data!["chart"] as Dictionary<string, object>;
            chart.Should().NotBeNull();
            
            var result = chart!["result"] as List<object>;
            result.Should().NotBeNull();
            result!.Count.Should().Be(1);
            
            var firstResult = result[0] as Dictionary<string, object>;
            firstResult.Should().NotBeNull();
            
            var meta = firstResult!["meta"] as Dictionary<string, object>;
            meta.Should().NotBeNull();
            meta!["currency"].Should().Be("USD");
            meta["symbol"].Should().Be("AMZN");
            meta["price"].Should().Be(88.505);
            
            var timestamp = firstResult["timestamp"] as List<object>;
            timestamp.Should().NotBeNull();
            timestamp!.Count.Should().Be(3);
            timestamp[0].Should().Be(863703000);
            timestamp[1].Should().Be(863789400);
            timestamp[2].Should().Be(864048600);
            
            var indicators = firstResult["indicators"] as List<object>;
            indicators.Should().NotBeNull();
            indicators!.Count.Should().Be(2);
            
            var closeIndicator = indicators[0] as Dictionary<string, object>;
            closeIndicator.Should().NotBeNull();
            var closeValues = closeIndicator!["close"] as List<object>;
            closeValues.Should().NotBeNull();
            closeValues!.Count.Should().Be(3);
            closeValues[0].Should().Be(1.0);
        }

        [Fact]
        public void Serialize_And_Deserialize_Document_With_Mixed_Array_Types()
        {
            var doc = new Document
            {
                Id = "mixed-test",
                Data = new Dictionary<string, object>
                {
                    ["numbers"] = new List<object> { 1, 2.5, 3 },
                    ["strings"] = new List<object> { "a", "b", "c" },
                    ["booleans"] = new List<object> { true, false, true },
                    ["mixed"] = new List<object> { 1, "text", true, null, new Dictionary<string, object> { ["key"] = "value" } }
                }
            };

            var bytes = CustomBinaryDocumentCodec.Serialize(doc);
            var doc2 = CustomBinaryDocumentCodec.Deserialize(bytes);

            doc2.Data.Should().NotBeNull();
            
            var numbers = doc2.Data!["numbers"] as List<object>;
            numbers.Should().NotBeNull();
            numbers!.Count.Should().Be(3);
            numbers[0].Should().Be(1);
            numbers[1].Should().Be(2.5);
            numbers[2].Should().Be(3);
            
            var strings = doc2.Data["strings"] as List<object>;
            strings.Should().NotBeNull();
            strings!.Count.Should().Be(3);
            strings[0].Should().Be("a");
            
            var mixed = doc2.Data["mixed"] as List<object>;
            mixed.Should().NotBeNull();
            mixed!.Count.Should().Be(5);
            mixed[0].Should().Be(1);
            mixed[1].Should().Be("text");
            mixed[2].Should().Be(true);
            mixed[3].Should().BeNull();
            
            var nestedDict = mixed[4] as Dictionary<string, object>;
            nestedDict.Should().NotBeNull();
            nestedDict!["key"].Should().Be("value");
        }
    }
}
