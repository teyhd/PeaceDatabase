using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Binary;
using PB = PeaceDatabase.Storage.Protobuf.ProtobufDocumentCodec;
using PeaceDatabase.Protos;
using Google.Protobuf;

namespace PeaceDatabase.WebApi.Controllers
{
    [ApiController]
    [Route("v1/bench")] 
    [Produces("application/json")]
    public sealed class BenchController : ControllerBase
    {
        public sealed class SourceConfig
        {
            [Required] public string Kind { get; set; } = "synthetic"; // synthetic|file|url
            public string? Path { get; set; }
            public string? Url { get; set; }
        }

        public sealed class BenchRequest
        {
            [Required] public SourceConfig Source { get; set; } = new();
            public List<int>? Scales { get; set; } = new() { 1, 10, 100, 1000, 10000, 100000 };
            public int MaxDocs { get; set; } = 100000;
            public int Warmup { get; set; } = 1;
            public int Iterations { get; set; } = 1;
            public int? RandomSeed { get; set; }
        }

        public sealed class BenchResult
        {
            public required string Format { get; set; }
            public required int N { get; set; }
            public double SerializeMs { get; set; }
            public double DeserializeMs { get; set; }
            public long TotalBytes { get; set; }
            public double AvgBytesPerDoc { get; set; }
        }

        public sealed class RunResponse
        {
            public required object Dataset { get; set; }
            public required int[] Scales { get; set; }
            public required List<BenchResult> Results { get; set; }
        }

        [HttpGet("formats")]
        public IActionResult Formats() => Ok(new[] { "json", "binary", "protobuf" });

        [HttpPost("run")]
        [ProducesResponseType(typeof(RunResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> Run([FromBody, Required] BenchRequest req)
        {
            // Load dataset
            List<Document> docs;
            try
            {
                docs = await LoadDatasetAsync(req.Source, req.MaxDocs, req.RandomSeed);
            }
            catch (Exception ex)
            {
                return BadRequest(new { ok = false, error = ex.Message, source = req.Source });
            }
            var scales = (req.Scales ?? new()).Distinct().OrderBy(x => x).Select(x => Math.Min(x, req.MaxDocs)).ToArray();

            var results = new List<BenchResult>();

            // Warmup
            Warmup(docs, Math.Min(docs.Count, 1000), req.Warmup);

            foreach (var n in scales)
            {
                var slice = docs.Take(n).ToList();
                results.Add(MeasureJson(slice, req.Iterations));
                results.Add(MeasureBinary(slice, req.Iterations));
                results.Add(MeasureProto(slice, req.Iterations));
                // Avro disabled for build stability
            }

            var meta = new
            {
                source = req.Source.Kind,
                path = req.Source.Path,
                url = req.Source.Url,
                total = docs.Count
            };

            return Ok(new RunResponse { Dataset = meta, Scales = scales, Results = results });
        }

        private static void Warmup(List<Document> docs, int n, int warmup)
        {
            if (warmup <= 0 || n <= 0) return;
            var slice = docs.Take(n).ToList();
            for (int w = 0; w < warmup; w++)
            {
                _ = MeasureJson(slice, 1);
                _ = MeasureBinary(slice, 1);
                _ = MeasureProto(slice, 1);
            }
        }

        private static BenchResult MeasureJson(List<Document> docs, int iterations)
        {
            var serSw = Stopwatch.StartNew();
            long totalBytes = 0;
            byte[][] last = Array.Empty<byte[]>();
            for (int k = 0; k < iterations; k++)
            {
                last = new byte[docs.Count][];
                for (int i = 0; i < docs.Count; i++)
                {
                    var b = JsonSerializer.SerializeToUtf8Bytes(docs[i]);
                    totalBytes += b.Length;
                    last[i] = b;
                }
            }
            serSw.Stop();

            var desSw = Stopwatch.StartNew();
            for (int k = 0; k < iterations; k++)
            {
                for (int i = 0; i < docs.Count; i++)
                {
                    var d = JsonSerializer.Deserialize<Document>(last[i]);
                    if (d == null) throw new Exception("json deser failed");
                }
            }
            desSw.Stop();

            return new BenchResult
            {
                Format = "json",
                N = docs.Count,
                SerializeMs = serSw.Elapsed.TotalMilliseconds,
                DeserializeMs = desSw.Elapsed.TotalMilliseconds,
                TotalBytes = totalBytes,
                AvgBytesPerDoc = docs.Count > 0 ? (double)totalBytes / docs.Count : 0
            };
        }

        private static BenchResult MeasureBinary(List<Document> docs, int iterations)
        {
            var serSw = Stopwatch.StartNew();
            long totalBytes = 0;
            byte[][] last = Array.Empty<byte[]>();
            for (int k = 0; k < iterations; k++)
            {
                last = new byte[docs.Count][];
                for (int i = 0; i < docs.Count; i++)
                {
                    var b = CustomBinaryDocumentCodec.Serialize(docs[i]);
                    totalBytes += b.Length;
                    last[i] = b;
                }
            }
            serSw.Stop();

            var desSw = Stopwatch.StartNew();
            for (int k = 0; k < iterations; k++)
            {
                for (int i = 0; i < docs.Count; i++)
                {
                    var d = CustomBinaryDocumentCodec.Deserialize(last[i]);
                    if (d == null) throw new Exception("bin deser failed");
                }
            }
            desSw.Stop();

            return new BenchResult
            {
                Format = "binary",
                N = docs.Count,
                SerializeMs = serSw.Elapsed.TotalMilliseconds,
                DeserializeMs = desSw.Elapsed.TotalMilliseconds,
                TotalBytes = totalBytes,
                AvgBytesPerDoc = docs.Count > 0 ? (double)totalBytes / docs.Count : 0
            };
        }

        private static BenchResult MeasureProto(List<Document> docs, int iterations)
        {
            var serSw = Stopwatch.StartNew();
            long totalBytes = 0;
            byte[][] last = Array.Empty<byte[]>();
            for (int k = 0; k < iterations; k++)
            {
                last = new byte[docs.Count][];
                for (int i = 0; i < docs.Count; i++)
                {
                    var msg = PB.ToProto(docs[i]);
                    var b = msg.ToByteArray();
                    totalBytes += b.Length;
                    last[i] = b;
                }
            }
            serSw.Stop();

            var desSw = Stopwatch.StartNew();
            for (int k = 0; k < iterations; k++)
            {
                for (int i = 0; i < docs.Count; i++)
                {
                    var m = DocumentMessage.Parser.ParseFrom(last[i]);
                    var d = PB.FromProto(m);
                    if (d == null) throw new Exception("pb deser failed");
                }
            }
            desSw.Stop();

            return new BenchResult
            {
                Format = "protobuf",
                N = docs.Count,
                SerializeMs = serSw.Elapsed.TotalMilliseconds,
                DeserializeMs = desSw.Elapsed.TotalMilliseconds,
                TotalBytes = totalBytes,
                AvgBytesPerDoc = docs.Count > 0 ? (double)totalBytes / docs.Count : 0
            };
        }

        // Avro measurement removed

        private static async Task<List<Document>> LoadDatasetAsync(SourceConfig source, int maxDocs, int? seed)
        {
            return source.Kind.ToLowerInvariant() switch
            {
                "synthetic" => GenerateSynthetic(maxDocs, seed),
                "file" => await LoadFromFileAsync(source.Path!, maxDocs),
                "url" => await LoadFromUrlAsync(source.Url!, maxDocs),
                _ => GenerateSynthetic(maxDocs, seed)
            };
        }

        private static List<Document> GenerateSynthetic(int maxDocs, int? seed)
        {
            var rnd = seed.HasValue ? new Random(seed.Value) : new Random(123);
            var list = new List<Document>(maxDocs);
            for (int i = 0; i < maxDocs; i++)
            {
                var d = new Document
                {
                    Id = $"syn-{i:D6}",
                    Data = new Dictionary<string, object>
                    {
                        ["name"] = $"user_{i}",
                        ["age"] = 18 + (i % 60),
                        ["score"] = rnd.NextDouble() * 100.0,
                        ["active"] = (i % 3) == 0,
                        ["tags"] = new List<string> { "kaggle", "bench", ((i%2)==0?"even":"odd") },
                        ["nested"] = new Dictionary<string, object>
                        {
                            ["x"] = rnd.Next(0, 1000),
                            ["y"] = rnd.NextDouble(),
                            ["z"] = new Dictionary<string, object> { ["k"] = "v" }
                        }
                    },
                    Tags = new List<string> { "synthetic" },
                    Content = string.Join(' ', Enumerable.Repeat("lorem ipsum", 2 + (i % 5)))
                };
                list.Add(d);
            }
            return list;
        }

        private static async Task<List<Document>> LoadFromFileAsync(string path, int maxDocs)
        {
            var abs = Path.IsPathRooted(path) ? path : Path.GetFullPath(path);
            if (!System.IO.File.Exists(abs))
                throw new FileNotFoundException($"File not found: {abs}");

            var list = new List<Document>(capacity: Math.Min(maxDocs, 10000));
            var ext = Path.GetExtension(abs).ToLowerInvariant();

            await using var fs = System.IO.File.OpenRead(abs);

            if (ext == ".json")
            {
                using var jd = await JsonDocument.ParseAsync(fs);
                var root = jd.RootElement;
                int idx = 0;
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                    {
                        if (list.Count >= maxDocs) break;
                        list.Add(ConvertJson(idx++, el));
                    }
                }
                else if (root.ValueKind == JsonValueKind.Object)
                {
                    list.Add(ConvertJson(idx++, root));
                }
                else
                {
                    throw new InvalidDataException("Unsupported JSON root (expected array or object)");
                }
            }
            else
            {
                using var sr = new StreamReader(fs, Encoding.UTF8);
                string? line;
                int idx = 0;
                while ((line = await sr.ReadLineAsync()) != null && list.Count < maxDocs)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        using var jd = JsonDocument.Parse(line);
                        list.Add(ConvertJson(idx++, jd.RootElement));
                    }
                    catch { /* skip bad line */ }
                }
            }

            return list;
        }

        private static async Task<List<Document>> LoadFromUrlAsync(string url, int maxDocs)
        {
            using var http = new HttpClient();
            using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync();
            using var sr = new StreamReader(stream, Encoding.UTF8);
            var list = new List<Document>(capacity: Math.Min(maxDocs, 10000));
            string? line; int idx = 0;
            while ((line = await sr.ReadLineAsync()) != null && list.Count < maxDocs)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                try
                {
                    using var jd = JsonDocument.Parse(line);
                    list.Add(ConvertJson(idx++, jd.RootElement));
                }
                catch { /* skip */ }
            }
            return list;
        }

        private static Document ConvertJson(int idx, JsonElement root)
        {
            var data = JsonToObject(root) as Dictionary<string, object> ?? new Dictionary<string, object>();
            return new Document
            {
                Id = $"ds-{idx:D6}",
                Data = data,
                Tags = null,
                Content = null
            };
        }

        private static object? JsonToObject(JsonElement je)
        {
            switch (je.ValueKind)
            {
                case JsonValueKind.Null:
                case JsonValueKind.Undefined: return null;
                case JsonValueKind.String: return je.GetString();
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.Number:
                    if (je.TryGetInt32(out var vi)) return vi;
                    if (je.TryGetInt64(out var vl)) return vl;
                    return je.GetDouble();
                case JsonValueKind.Array:
                {
                    var list = new List<object?>();
                    foreach (var it in je.EnumerateArray()) list.Add(JsonToObject(it));
                    return list;
                }
                case JsonValueKind.Object:
                {
                    var dict = new Dictionary<string, object>(System.StringComparer.Ordinal);
                    foreach (var prop in je.EnumerateObject()) dict[prop.Name] = JsonToObject(prop.Value)!;
                    return dict;
                }
                default: return null;
            }
        }
    }
}


