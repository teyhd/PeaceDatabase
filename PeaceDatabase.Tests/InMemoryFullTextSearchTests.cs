using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.InMemory;

namespace PeaceDatabase.Tests.Storage
{
    public class InMemoryFullTextSearchTests
    {
        private readonly ITestOutputHelper _output;
        public InMemoryFullTextSearchTests(ITestOutputHelper output) => _output = output;

        [Fact]
        public void FullTextSearch_AddThreeDocs_FindMatches_And_MeasureTime()
        {
            // Arrange
            var svc = new InMemoryDocumentService();
            var db = "ft_db";
            svc.CreateDb(db).Ok.Should().BeTrue();

            SeedThreeDocs(svc, db);

            // Прогрев
            _ = svc.FullTextSearch(db, "full text").ToList();

            // Act #1: AND-поиск по всем словам
            var query1 = "full text indexing databases";
            var sw1 = Stopwatch.StartNew();
            var results1 = svc.FullTextSearch(db, query1).ToList();
            sw1.Stop();

            // Assert #1: найдётся хотя бы Intro...
            results1.Should().NotBeNull();
            results1.Count.Should().BeGreaterThan(0, "AND-поиск должен вернуть хотя бы Intro-док");
            var titles1 = Titles(results1);
            //titles1.Should().Contain(t => t!.Contains("Intro", StringComparison.OrdinalIgnoreCase));
            titles1.Should().Contain(t => t.Contains("Intro", StringComparison.OrdinalIgnoreCase));

            // Лог времени
            _output.WriteLine($"FT #1: \"{query1}\" -> {results1.Count} docs, {sw1.Elapsed.TotalMilliseconds:F2} ms");
            sw1.ElapsedMilliseconds.Should().BeLessThan(500, "на маленьком корпусе поиск должен быть быстрым");

            // Act #2: более «широкий» запрос, где попадут Intro и Indexing
            var query2 = "indexing databases";
            var sw2 = Stopwatch.StartNew();
            var results2 = svc.FullTextSearch(db, query2).ToList();
            sw2.Stop();

            // Assert #2: теперь ждём, что в выдаче будут оба заголовка
            var titles2 = Titles(results2);
            titles2.Should().Contain(t => t!.Contains("Intro", StringComparison.OrdinalIgnoreCase));
            titles2.Should().Contain(t => t!.Contains("Indexing", StringComparison.OrdinalIgnoreCase));

            _output.WriteLine($"FT #2: \"{query2}\" -> {results2.Count} docs, {sw2.Elapsed.TotalMilliseconds:F2} ms");
            sw2.ElapsedMilliseconds.Should().BeLessThan(500, "на маленьком корпусе поиск должен быть быстрым");
        }

        [Fact]
        public void FullTextSearch_Microbenchmark_50Runs_Avg_And_Median()
        {
            // Arrange
            var svc = new InMemoryDocumentService();
            var db = "ft_db_bench";
            svc.CreateDb(db).Ok.Should().BeTrue();
            SeedThreeDocs(svc, db);

            // Прогрев
            _ = svc.FullTextSearch(db, "full text").ToList();

            // Набор запросов (чередуем релевантные/около- и промахи)
            var queries = new[]
            {
                "full text indexing databases",
                "indexing",
                "token",
                "bench performance",
                "engines documents",
                "speed search",
                "relevance",
                "n-gram",
                "nonexistentzzz",   // промах
                "gibberish123"      // промах
            };

            // 50 прогонов
            const int runs = 50;
            var timesMs = new List<double>(runs);
            var rnd = new Random(42);

            // Сбросить шум GC перед серией
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            for (int i = 0; i < runs; i++)
            {
                var q = queries[rnd.Next(queries.Length)];
                var sw = Stopwatch.StartNew();
                var res = svc.FullTextSearch(db, q).ToList();
                sw.Stop();

                // базовая проверка корректности: не должно падать
                res.Should().NotBeNull();

                timesMs.Add(sw.Elapsed.TotalMilliseconds);
                if (i < 3) _output.WriteLine($"Run {i + 1:D2}: \"{q}\" -> {res.Count} hits, {sw.Elapsed.TotalMilliseconds:F2} ms");
            }

            // Метрики
            var avg = timesMs.Average();
            var med = Median(timesMs);
            var p95 = Percentile(timesMs, 0.95);

            _output.WriteLine($"Runs: {runs}");
            _output.WriteLine($"Avg: {avg:F2} ms; Median: {med:F2} ms; P95: {p95:F2} ms");
            _output.WriteLine($"All: [{string.Join(", ", timesMs.Select(t => t.ToString("F2")))}]");

            // Мягкие пороги для маленького корпуса в памяти — подстрой при необходимости
            avg.Should().BeLessThan(1.5, "среднее время запроса по 3 докам должно быть < 1.5 ms на типичной машине");
            p95.Should().BeLessThan(5.0, "95-й перцентиль должен быть стабильным для маленького корпуса");
        }

        // ---------- helpers ----------
        private static IReadOnlyList<Document> SeedThreeDocs(InMemoryDocumentService svc, string db)
        {
            var docs = new[]
            {
                new Document {
                    Data = new Dictionary<string, object> {
                        ["title"]   = "Intro to Document Databases",
                        ["content"] = "engines store documents. Full-text indexing helps finding words quickly."
                    }
                },
                new Document {
                    Data = new Dictionary<string, object> {
                        ["title"]   = "Indexing Strategies",
                        ["content"] = "N-gram and token-based indexing can speed up search. Databases differ by trade-offs."
                    }
                },
                new Document {
                    Data = new Dictionary<string, object> {
                        ["title"]   = "Practical Notes",
                        ["content"] = "We benchmark full text search to verify performance and relevance in a small dataset."
                    }
                },
            };
            foreach (var d in docs)
            {
                var (ok, ret, err) = svc.Post(db, d);
                ok.Should().BeTrue($"post must succeed (err: {err})");
                ret!.Id.Should().NotBeNullOrEmpty();
            }
            return docs;
        }

        private static string[] Titles(IEnumerable<Document> docs) =>
            docs.Select(r => r.Data != null && r.Data.TryGetValue("title", out var t) ? t?.ToString() : null)
                .Where(t => !string.IsNullOrWhiteSpace(t))
                .ToArray();

        private static double Median(List<double> xs)
        {
            if (xs.Count == 0) return 0;
            var arr = xs.OrderBy(v => v).ToArray();
            int n = arr.Length;
            return (n % 2 == 1) ? arr[n / 2] : (arr[n / 2 - 1] + arr[n / 2]) / 2.0;
        }

        private static double Percentile(List<double> xs, double p)
        {
            if (xs.Count == 0) return 0;
            var arr = xs.OrderBy(v => v).ToArray();
            var rank = p * (arr.Length - 1);
            var lo = (int)Math.Floor(rank);
            var hi = (int)Math.Ceiling(rank);
            if (lo == hi) return arr[lo];
            var frac = rank - lo;
            return arr[lo] + (arr[hi] - arr[lo]) * frac;
        }
    }
}
