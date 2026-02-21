using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Protos;
using PeaceDatabase.Storage.Binary;
using PeaceDatabase.Storage.Compact;
using PB = PeaceDatabase.Storage.Protobuf.ProtobufDocumentCodec;

namespace PeaceDatabase.WebApi.Controllers
{
    public sealed class LoadStats
    {
        public string FilePath { get; set; } = "";
        public long FileSizeBytes { get; set; }
        public string Sha256 { get; set; } = "";
        public int ParsedDocuments { get; set; }
        public int SkippedBadLines { get; set; }
        public bool HitMaxDocs { get; set; }          // ������������ ��-�� maxDocs
        public bool ReachedEndOfFile { get; set; }    // ����� �� ����� �����
        public bool ReadInFull => ReachedEndOfFile && !HitMaxDocs;
    }

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

        #region Elias-Fano Compact Index Benchmark

        public sealed class CompactBenchRequest
        {
            /// <summary>Масштабы: количество документов для тестирования</summary>
            public List<int>? Scales { get; set; } = new() { 100, 1000, 10000, 10000 };
            /// <summary>Токенов на документ</summary>
            public int TokensPerDoc { get; set; } = 100;
            /// <summary>Размер словаря (уникальных токенов)</summary>
            public int VocabularySize { get; set; } = 1000;
            /// <summary>Количество поисковых запросов</summary>
            public int Queries { get; set; } = 100;
            /// <summary>Seed для воспроизводимости</summary>
            public int? RandomSeed { get; set; } = 42;
        }

        public sealed class CompactMemoryResult
        {
            public int NumDocs { get; set; }
            public int TokensPerDoc { get; set; }
            public double HashSetKb { get; set; }
            public double EliasFanoKb { get; set; }
            public double CompressionRatio { get; set; }
            public long TotalPostings { get; set; }
            public double BitsPerPosting { get; set; }
        }

        public sealed class CompactSpeedResult
        {
            public int NumDocs { get; set; }
            public double HashSetMs { get; set; }
            public double EliasFanoMs { get; set; }
            public int Queries { get; set; }
            public int HashSetResults { get; set; }
            public int EliasFanoResults { get; set; }
        }

        public sealed class CompactBenchResponse
        {
            public required CompactBenchRequest Config { get; set; }
            public required List<CompactMemoryResult> Memory { get; set; }
            public required List<CompactSpeedResult> Speed { get; set; }
        }

        /// <summary>
        /// Запускает бенчмарк сравнения HashSet vs Elias-Fano для полнотекстового индекса.
        /// </summary>
        [HttpPost("compact")]
        [ProducesResponseType(typeof(CompactBenchResponse), StatusCodes.Status200OK)]
        public IActionResult RunCompactBench([FromBody] CompactBenchRequest? req)
        {
            req ??= new CompactBenchRequest();
            var random = new Random(req.RandomSeed ?? 42);

            var scales = (req.Scales ?? new List<int> { 100, 1000, 10000 })
                .Distinct()
                .OrderBy(x => x)
                .ToList();

            var vocabulary = Enumerable.Range(0, req.VocabularySize)
                .Select(i => $"word{i}")
                .ToList();

            var memoryResults = new List<CompactMemoryResult>();
            var speedResults = new List<CompactSpeedResult>();

            foreach (var numDocs in scales)
            {
                // Генерируем документы
                var documents = GenerateTokenizedDocs(numDocs, req.TokensPerDoc, vocabulary, random);

                // === Сравнение памяти ===
                var (hashSetIndex, compactIndex) = BuildBothIndexes(documents);
                var memResult = MeasureMemory(numDocs, req.TokensPerDoc, hashSetIndex, compactIndex);
                memoryResults.Add(memResult);

                // === Сравнение скорости ===
                var queries = GenerateQueries(req.Queries, vocabulary, random);
                var speedResult = MeasureSpeed(numDocs, hashSetIndex, compactIndex, queries);
                speedResults.Add(speedResult);
            }

            return Ok(new CompactBenchResponse
            {
                Config = req,
                Memory = memoryResults,
                Speed = speedResults
            });
        }

        private static List<(string DocId, List<string> Tokens)> GenerateTokenizedDocs(
            int numDocs, int tokensPerDoc, List<string> vocabulary, Random random)
        {
            return Enumerable.Range(0, numDocs)
                .Select(i =>
                {
                    var tokens = vocabulary
                        .OrderBy(_ => random.Next())
                        .Take(tokensPerDoc)
                        .Distinct()
                        .ToList();
                    return ($"doc-{i}", tokens);
                })
                .ToList();
        }

        private static (Dictionary<string, HashSet<string>> HashSet, CompactFullTextIndex Compact) BuildBothIndexes(
            List<(string DocId, List<string> Tokens)> documents)
        {
            var hashSetIndex = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var compactIndex = new CompactFullTextIndex();

            foreach (var (docId, tokens) in documents)
            {
                foreach (var token in tokens)
                {
                    if (!hashSetIndex.TryGetValue(token, out var set))
                        hashSetIndex[token] = set = new HashSet<string>(StringComparer.Ordinal);
                    set.Add(docId);
                }
                compactIndex.Index(docId, tokens);
            }
            compactIndex.Compact();

            return (hashSetIndex, compactIndex);
        }

        private static CompactMemoryResult MeasureMemory(
            int numDocs, int tokensPerDoc,
            Dictionary<string, HashSet<string>> hashSetIndex,
            CompactFullTextIndex compactIndex)
        {
            // Оценка HashSet: ~80 bytes per entry (object header + hash + pointers + string)
            long hashSetBytes = 0;
            long totalPostings = 0;
            foreach (var kv in hashSetIndex)
            {
                hashSetBytes += 40 + kv.Key.Length * 2; // key string
                foreach (var val in kv.Value)
                {
                    hashSetBytes += 40 + 40 + val.Length * 2; // entry + value string
                    totalPostings++;
                }
            }

            var stats = compactIndex.GetStats();

            return new CompactMemoryResult
            {
                NumDocs = numDocs,
                TokensPerDoc = tokensPerDoc,
                HashSetKb = hashSetBytes / 1024.0,
                EliasFanoKb = stats.TotalSizeBytes / 1024.0,
                CompressionRatio = hashSetBytes > 0 ? (double)hashSetBytes / (stats.TotalSizeBytes + 1) : 0,
                TotalPostings = totalPostings,
                BitsPerPosting = totalPostings > 0 ? stats.TotalSizeBytes * 8.0 / totalPostings : 0
            };
        }

        private static List<string[]> GenerateQueries(int count, List<string> vocabulary, Random random)
        {
            return Enumerable.Range(0, count)
                .Select(_ => vocabulary
                    .OrderBy(__ => random.Next())
                    .Take(random.Next(1, 4))
                    .ToArray())
                .ToList();
        }

        private static CompactSpeedResult MeasureSpeed(
            int numDocs,
            Dictionary<string, HashSet<string>> hashSetIndex,
            CompactFullTextIndex compactIndex,
            List<string[]> queries)
        {
            // Warmup
            foreach (var q in queries.Take(10))
            {
                SearchHashSet(hashSetIndex, q);
                compactIndex.Search(q);
            }

            // HashSet benchmark
            var swHashSet = Stopwatch.StartNew();
            int hashSetResults = 0;
            foreach (var q in queries)
            {
                hashSetResults += SearchHashSet(hashSetIndex, q).Count;
            }
            swHashSet.Stop();

            // Elias-Fano benchmark (using Search which unpacks to list first)
            var swCompact = Stopwatch.StartNew();
            int compactResults = 0;
            foreach (var q in queries)
            {
                compactResults += compactIndex.Search(q).Count;
            }
            swCompact.Stop();

            return new CompactSpeedResult
            {
                NumDocs = numDocs,
                HashSetMs = swHashSet.Elapsed.TotalMilliseconds,
                EliasFanoMs = swCompact.Elapsed.TotalMilliseconds,
                Queries = queries.Count,
                HashSetResults = hashSetResults,
                EliasFanoResults = compactResults
            };
        }

        private static List<string> SearchHashSet(Dictionary<string, HashSet<string>> index, string[] tokens)
        {
            HashSet<string>? result = null;
            foreach (var token in tokens)
            {
                if (!index.TryGetValue(token, out var set))
                    return new List<string>();

                if (result == null)
                    result = new HashSet<string>(set, StringComparer.Ordinal);
                else
                    result.IntersectWith(set);

                if (result.Count == 0)
                    return new List<string>();
            }
            return result?.ToList() ?? new List<string>();
        }

        #endregion

        [HttpPost("run")]
        [ProducesResponseType(typeof(RunResponse), StatusCodes.Status200OK)]
        public async Task<IActionResult> Run([FromBody, Required] BenchRequest req, CancellationToken ct)
        {
            // Load dataset
            List<Document> docs;
            LoadStats? stats;
            try
            {
                (docs, stats) = await LoadDatasetAsync(req.Source, req.MaxDocs, req.RandomSeed, ct);
            }
            catch (Exception ex)
            {
                return BadRequest(new { ok = false, error = ex.Message, source = req.Source });
            }

            var scales = (req.Scales ?? new())
                .Distinct()
                .OrderBy(x => x)
                .Select(x => Math.Min(x, req.MaxDocs))
                .ToArray();

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
                total = docs.Count,
                fileSizeBytes = stats?.FileSizeBytes,
                sha256 = stats?.Sha256,
                parsed = stats?.ParsedDocuments,
                skippedBadLines = stats?.SkippedBadLines,
                reachedEndOfFile = stats?.ReachedEndOfFile,
                hitMaxDocs = stats?.HitMaxDocs,
                readInFull = stats?.ReadInFull
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
                    // Use JsonSerializer with WriteIndented=false to ensure compact serialization
                    // and consistent handling of nested structures
                    var options = new JsonSerializerOptions 
                    { 
                        WriteIndented = false,
                        DefaultBufferSize = 1 << 20
                    };
                    var b = JsonSerializer.SerializeToUtf8Bytes(docs[i], options);
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

        // ---------------- Dataset loading ----------------

        private static async Task<(List<Document> Docs, LoadStats? Stats)> LoadDatasetAsync(
            SourceConfig source, int maxDocs, int? seed, CancellationToken ct)
        {
            switch (source.Kind.ToLowerInvariant())
            {
                case "synthetic":
                    return (GenerateSynthetic(maxDocs, seed), null);

                case "file":
                    {
                        var (docs, stats) = await LoadFromFileAsync(source.Path!, maxDocs, ct);
                        return (docs, stats);
                    }

                case "url":
                    {
                        var (docs, stats) = await LoadFromUrlAsync(source.Url!, maxDocs, ct);
                        return (docs, stats);
                    }

                default:
                    return (GenerateSynthetic(maxDocs, seed), null);
            }
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
                        ["tags"] = new List<string> { "kaggle", "bench", ((i % 2) == 0 ? "even" : "odd") },
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

        // ��������� �������� .json (array/object) � JSONL � ������������
        private static async Task<(List<Document> Docs, LoadStats Stats)> LoadFromFileAsync(
            string path, int maxDocs, CancellationToken ct)
        {
            var abs = System.IO.Path.IsPathRooted(path) ? path : System.IO.Path.GetFullPath(path);
            if (!System.IO.File.Exists(abs))
                throw new System.IO.FileNotFoundException($"File not found: {abs}");

            // ��� � ������ ����� (��� �������� �����������)
            string sha256;
            long size;
            using (var fsHash = System.IO.File.Open(abs, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
            {
                size = fsHash.Length;
                using var sha = SHA256.Create();
                var hash = await sha.ComputeHashAsync(fsHash, ct);
                sha256 = Convert.ToHexString(hash);
            }

            var list = new List<Document>(capacity: Math.Min(maxDocs, 10000));
            var stats = new LoadStats { FilePath = abs, FileSizeBytes = size, Sha256 = sha256 };

            var ext = System.IO.Path.GetExtension(abs).ToLowerInvariant();
            await using var fs = System.IO.File.Open(abs, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);

            if (ext == ".json")
            {
                // ���������� ������: ������ ��� ������
                fs.Seek(0, System.IO.SeekOrigin.Begin);
                int first;
                do { first = fs.ReadByte(); } while (first is 0x20 or 0x0A or 0x0D or 0x09); // �������/CR/LF/TAB

                if (first == -1)
                {
                    stats.ParsedDocuments = 0;
                    stats.ReachedEndOfFile = true;
                    return (list, stats);
                }

                fs.Seek(-1, System.IO.SeekOrigin.Current);

                if (first == (int)'[')
                {
                    var opts = new JsonSerializerOptions { DefaultBufferSize = 1 << 20 };
                    await foreach (var el in JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(fs, opts, ct))
                    {
                        if (list.Count >= maxDocs) { stats.HitMaxDocs = true; break; }
                        list.Add(ConvertJson(list.Count, el));
                    }
                    stats.ParsedDocuments = list.Count;
                    stats.ReachedEndOfFile = fs.Position == fs.Length;
                }
                else if (first == (int)'{')
                {
                    var el = await JsonSerializer.DeserializeAsync<JsonElement>(fs, options: null, ct);
                    list.Add(ConvertJson(0, el));
                    stats.ParsedDocuments = 1;
                    stats.ReachedEndOfFile = true;
                }
                else
                {
                    throw new System.IO.InvalidDataException("Unsupported JSON root (expected array '[' or object '{').");
                }
            }
            else
            {
                // JSONL / NDJSON
                using var sr = new System.IO.StreamReader(fs, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1 << 16);
                string? line;
                int skipped = 0;

                while ((line = await sr.ReadLineAsync()) != null)
                {
                    ct.ThrowIfCancellationRequested();
                    if (list.Count >= maxDocs) { stats.HitMaxDocs = true; break; }
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    try
                    {
                        using var jd = JsonDocument.Parse(line);
                        list.Add(ConvertJson(list.Count, jd.RootElement));
                    }
                    catch (JsonException)
                    {
                        skipped++;
                    }
                }

                stats.ParsedDocuments = list.Count;
                stats.SkippedBadLines = skipped;
                stats.ReachedEndOfFile = sr.EndOfStream || fs.Position == fs.Length;
            }

            return (list, stats);
        }

        private static async Task<(List<Document> Docs, LoadStats Stats)> LoadFromUrlAsync(
            string url, int maxDocs, CancellationToken ct)
        {
            using var http = new HttpClient();
            using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
            resp.EnsureSuccessStatusCode();

            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            using var sr = new System.IO.StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1 << 16);

            var list = new List<Document>(capacity: Math.Min(maxDocs, 10000));
            string? line;
            int skipped = 0;

            while ((line = await sr.ReadLineAsync()) != null)
            {
                ct.ThrowIfCancellationRequested();
                if (list.Count >= maxDocs) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    using var jd = JsonDocument.Parse(line);
                    list.Add(ConvertJson(list.Count, jd.RootElement));
                }
                catch (JsonException)
                {
                    skipped++;
                }
            }

            // ��� URL ������/��� ����� �� �������
            var stats = new LoadStats
            {
                FilePath = url,
                FileSizeBytes = 0,
                Sha256 = "",
                ParsedDocuments = list.Count,
                SkippedBadLines = skipped,
                HitMaxDocs = list.Count >= maxDocs,
                ReachedEndOfFile = sr.EndOfStream
            };

            return (list, stats);
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
                        var dict = new Dictionary<string, object>(StringComparer.Ordinal);
                        foreach (var prop in je.EnumerateObject())
                        {
                            var val = JsonToObject(prop.Value);
                            if (val != null) dict[prop.Name] = val;
                        }
                        return dict;
                    }
                default: return null;
            }
        }
    }
}
