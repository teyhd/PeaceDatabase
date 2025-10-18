using System;
using System.Collections.Generic;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Protobuf;
using Xunit;

namespace PeaceDatabase.Tests
{
    public class ProtobufDocumentCodecNestedTests
    {
        [Fact]
        public void Protobuf_Diagnostics_Nested_Structure()
        {
            var doc = new Document
            {
                Id = "amzn-proto",
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
                                    ["price"] = 88.505,
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
            void PrintDict(Dictionary<string, object> d, string prefix = "")
            {
                foreach (var kv in d)
                {
                    var type = kv.Value?.GetType().Name ?? "null";
                    Console.WriteLine($"{prefix}{kv.Key}: {type}");
                    if (kv.Value is Dictionary<string, object> childDict)
                        PrintDict(childDict, prefix+"  ");
                    else if (kv.Value is List<object> list)
                        PrintList(list, prefix+"  ");
                }
            }
            void PrintList(List<object> list, string prefix="")
            {
                foreach (var item in list)
                {
                    var type = item?.GetType().Name ?? "null";
                    Console.WriteLine($"{prefix}[LIST] {type}");
                    if (item is Dictionary<string, object> dict)
                        PrintDict(dict, prefix+"    ");
                    else if (item is List<object> l)
                        PrintList(l, prefix+"    ");
                }
            }
            Console.WriteLine("--- Input dictionary types:");
            PrintDict(doc.Data!);
            var proto = ProtobufDocumentCodec.ToProto(doc);
            Console.WriteLine("--- Protobuf Struct Fields:");
            foreach(var kv in proto.Data.Fields)
            {
                Console.WriteLine($"Field: {kv.Key}, Kind: {kv.Value.KindCase}, Data: {kv.Value}");
            }
        }
    }
}
