using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Compact;
using PeaceDatabase.Storage.InMemory;

namespace PeaceDatabase.Tests.Compact
{
    /// <summary>
    /// Тесты для сравнения Elias-Fano компактного индекса с обычным HashSet индексом.
    /// Практическая работа № 5: Компактные структуры данных.
    /// </summary>
    public class EliasFanoBenchmarkTests
    {
        private readonly ITestOutputHelper _output;

        public EliasFanoBenchmarkTests(ITestOutputHelper output) => _output = output;

        #region Unit Tests - EliasFanoList

        [Fact]
        public void EliasFanoList_SmallSequence_CorrectlyEncodesAndDecodes()
        {
            // Arrange
            var values = new[] { 1, 5, 10, 15, 20, 100, 500, 1000 };

            // Act
            var ef = new EliasFanoList(values);

            // Assert
            ef.Count.Should().Be(values.Length);
            ef.MaxValue.Should().Be(1000);

            for (int i = 0; i < values.Length; i++)
            {
                ef[i].Should().Be(values[i], $"element at index {i}");
            }

            _output.WriteLine($"EliasFano: {ef.Count} elements, {ef.BitsPerElement:F2} bits/element");
        }

        [Fact]
        public void EliasFanoList_Contains_WorksCorrectly()
        {
            // Arrange
            var values = Enumerable.Range(0, 1000).Where(i => i % 3 == 0).ToArray();
            var ef = new EliasFanoList(values);

            // Act & Assert
            foreach (var v in values)
                ef.Contains(v).Should().BeTrue($"{v} should be found");

            ef.Contains(1).Should().BeFalse("1 is not divisible by 3");
            ef.Contains(2).Should().BeFalse("2 is not divisible by 3");
            ef.Contains(10000).Should().BeFalse("10000 is out of range");
        }

        [Fact]
        public void EliasFanoList_NextGEQ_WorksCorrectly()
        {
            // Arrange
            var values = new[] { 10, 20, 30, 40, 50 };
            var ef = new EliasFanoList(values);

            // Act & Assert
            ef.NextGEQValue(0).Should().Be(10);
            ef.NextGEQValue(10).Should().Be(10);
            ef.NextGEQValue(11).Should().Be(20);
            ef.NextGEQValue(25).Should().Be(30);
            ef.NextGEQValue(50).Should().Be(50);
            ef.NextGEQValue(51).Should().Be(-1);
        }

        [Fact]
        public void EliasFanoList_Intersection_WorksCorrectly()
        {
            // Arrange
            var a = new EliasFanoList(new[] { 1, 3, 5, 7, 9, 11, 13 });
            var b = new EliasFanoList(new[] { 2, 3, 5, 8, 11, 14 });

            // Act
            var intersection = EliasFanoList.Intersect(a, b);

            // Assert
            intersection.Should().BeEquivalentTo(new[] { 3, 5, 11 });
        }

        #endregion

        #region Unit Tests - CompactBitVector

        [Fact]
        public void CompactBitVector_Rank_ReturnsCorrectCount()
        {
            // Arrange: битовый вектор 1010 1100 (позиции 0,2,5,6 установлены)
            var builder = new BitVectorBuilder(8);
            builder.AppendOne();   // 0
            builder.Append(false); // 1
            builder.AppendOne();   // 2
            builder.Append(false); // 3
            builder.Append(false); // 4
            builder.AppendOne();   // 5
            builder.AppendOne();   // 6
            builder.Append(false); // 7

            var bv = builder.Build();

            // Act & Assert
            bv.Rank(0).Should().Be(0, "no bits before position 0");
            bv.Rank(1).Should().Be(1, "one '1' before position 1");
            bv.Rank(3).Should().Be(2, "two '1's before position 3");
            bv.Rank(6).Should().Be(3, "three '1's before position 6");
            bv.Rank(8).Should().Be(4, "four '1's in total");
        }

        [Fact]
        public void CompactBitVector_Select_ReturnsCorrectPosition()
        {
            // Arrange
            var builder = new BitVectorBuilder(8);
            builder.AppendOne();   // 0
            builder.Append(false); // 1
            builder.AppendOne();   // 2
            builder.Append(false); // 3
            builder.Append(false); // 4
            builder.AppendOne();   // 5
            builder.AppendOne();   // 6
            builder.Append(false); // 7

            var bv = builder.Build();

            // Act & Assert
            bv.Select(0).Should().Be(0, "0th '1' at position 0");
            bv.Select(1).Should().Be(2, "1st '1' at position 2");
            bv.Select(2).Should().Be(5, "2nd '1' at position 5");
            bv.Select(3).Should().Be(6, "3rd '1' at position 6");
            bv.Select(4).Should().Be(-1, "no 4th '1'");
        }

        #endregion

        #region Integration Tests - CompactFullTextIndex

        [Fact]
        public void CompactFullTextIndex_IndexAndSearch_ReturnsCorrectDocuments()
        {
            // Arrange
            var index = new CompactFullTextIndex();
            index.Index("doc1", new[] { "hello", "world", "test" });
            index.Index("doc2", new[] { "hello", "foo", "bar" });
            index.Index("doc3", new[] { "world", "test", "baz" });
            index.Compact();

            // Act & Assert
            var result1 = index.SearchCompact(new[] { "hello" });
            result1.Should().BeEquivalentTo(new[] { "doc1", "doc2" });

            var result2 = index.SearchCompact(new[] { "hello", "world" });
            result2.Should().BeEquivalentTo(new[] { "doc1" });

            var result3 = index.SearchCompact(new[] { "test" });
            result3.Should().BeEquivalentTo(new[] { "doc1", "doc3" });

            var result4 = index.SearchCompact(new[] { "nonexistent" });
            result4.Should().BeEmpty();
        }

        #endregion

        #region Benchmarks - Memory Comparison

        [Theory]
        [InlineData(100, 10)]
        [InlineData(1000, 50)]
        [InlineData(10000, 100)]
        public void Benchmark_MemoryComparison_EliasFanoVsHashSet(int numDocs, int tokensPerDoc)
        {
            // Arrange
            var random = new Random(42);
            var vocabulary = GenerateVocabulary(1000);
            var documents = GenerateDocuments(numDocs, tokensPerDoc, vocabulary, random);

            // === HashSet-based index ===
            var hashSetIndex = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            foreach (var (docId, tokens) in documents)
            {
                foreach (var token in tokens)
                {
                    if (!hashSetIndex.TryGetValue(token, out var set))
                        hashSetIndex[token] = set = new HashSet<string>(StringComparer.Ordinal);
                    set.Add(docId);
                }
            }

            // === Elias-Fano index ===
            var compactIndex = new CompactFullTextIndex();
            foreach (var (docId, tokens) in documents)
            {
                compactIndex.Index(docId, tokens);
            }
            compactIndex.Compact();

            // Measure
            var stats = compactIndex.GetStats();
            long hashSetEstimate = EstimateHashSetMemory(hashSetIndex);

            _output.WriteLine($"=== Memory Comparison: {numDocs} docs, {tokensPerDoc} tokens/doc ===");
            _output.WriteLine($"HashSet index:     ~{hashSetEstimate / 1024.0:F1} KB");
            _output.WriteLine($"Elias-Fano index:  ~{stats.TotalSizeBytes / 1024.0:F1} KB");
            _output.WriteLine($"Compression ratio: {(double)hashSetEstimate / (stats.TotalSizeBytes + 1):F2}x");
            _output.WriteLine($"Total postings:    {stats.TotalPostings}");
            _output.WriteLine($"Bits per posting:  {(stats.TotalPostings > 0 ? stats.TotalSizeBytes * 8.0 / stats.TotalPostings : 0):F2}");

            // Assert - compact index should be smaller
            stats.TotalSizeBytes.Should().BeLessThan(hashSetEstimate,
                "Elias-Fano should use less memory than HashSet");
        }

        [Fact]
        public void Benchmark_DetailedMemoryAnalysis()
        {
            // Large-scale test
            var random = new Random(42);
            var vocabulary = GenerateVocabulary(5000);
            var documents = GenerateDocuments(10000, 50, vocabulary, random);

            var compactIndex = new CompactFullTextIndex();
            foreach (var (docId, tokens) in documents)
            {
                compactIndex.Index(docId, tokens);
            }
            compactIndex.Compact();

            var stats = compactIndex.GetStats();

            _output.WriteLine("=== Detailed Memory Analysis ===");
            _output.WriteLine(stats.ToString());
            _output.WriteLine($"Average bits per posting: {(stats.TotalPostings > 0 ? stats.TotalSizeBytes * 8.0 / stats.TotalPostings : 0):F2}");
            _output.WriteLine($"Theoretical minimum (log2(n/posting)): ~2-3 bits");

            // Elias-Fano theoretical: ~2 + log2(U/n) bits per element
            // For typical posting lists, this should be 3-5 bits per element
            double bitsPerPosting = stats.TotalPostings > 0 ? stats.TotalSizeBytes * 8.0 / stats.TotalPostings : 0;
            bitsPerPosting.Should().BeLessThan(50, "Elias-Fano should be very compact");
        }

        #endregion

        #region Benchmarks - Speed Comparison

        [Theory]
        [InlineData(1000, 20)]
        [InlineData(5000, 30)]
        public void Benchmark_SearchSpeed_EliasFanoVsHashSet(int numDocs, int tokensPerDoc)
        {
            // Arrange
            var random = new Random(42);
            var vocabulary = GenerateVocabulary(500);
            var documents = GenerateDocuments(numDocs, tokensPerDoc, vocabulary, random);

            // Build both indexes
            var hashSetIndex = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var compactIndex = new CompactFullTextIndex();

            foreach (var (docId, tokens) in documents)
            {
                // Используем уникальные токены для обоих индексов
                var uniqueTokens = tokens.Distinct().ToList();
                foreach (var token in uniqueTokens)
                {
                    if (!hashSetIndex.TryGetValue(token, out var set))
                        hashSetIndex[token] = set = new HashSet<string>(StringComparer.Ordinal);
                    set.Add(docId);
                }
                compactIndex.Index(docId, uniqueTokens);
            }
            compactIndex.Compact();

            // Generate queries
            var queries = Enumerable.Range(0, 100)
                .Select(_ => vocabulary.OrderBy(__ => random.Next()).Take(random.Next(1, 4)).ToArray())
                .ToList();

            // Warmup
            foreach (var q in queries.Take(10))
            {
                SearchHashSet(hashSetIndex, q);
                compactIndex.SearchCompact(q);
            }

            // Benchmark HashSet
            var swHashSet = Stopwatch.StartNew();
            int hashSetResults = 0;
            foreach (var q in queries)
            {
                hashSetResults += SearchHashSet(hashSetIndex, q).Count;
            }
            swHashSet.Stop();

            // Benchmark Elias-Fano
            var swCompact = Stopwatch.StartNew();
            int compactResults = 0;
            foreach (var q in queries)
            {
                compactResults += compactIndex.SearchCompact(q).Count;
            }
            swCompact.Stop();

            _output.WriteLine($"=== Search Speed Comparison: {numDocs} docs ===");
            _output.WriteLine($"HashSet:     {swHashSet.ElapsedMilliseconds} ms ({queries.Count} queries, {hashSetResults} total results)");
            _output.WriteLine($"Elias-Fano:  {swCompact.ElapsedMilliseconds} ms ({queries.Count} queries, {compactResults} total results)");
            _output.WriteLine($"Speed ratio: {(double)swCompact.ElapsedMilliseconds / (swHashSet.ElapsedMilliseconds + 1):F2}x");

            // Both indexes should return results (not necessarily equal due to ordering)
            hashSetResults.Should().BeGreaterThan(0, "HashSet should find results");
            compactResults.Should().BeGreaterThan(0, "Elias-Fano should find results");
        }

        [Fact]
        public void Benchmark_IntegratedWithDocumentService()
        {
            // Arrange - use actual InMemoryDocumentService
            var service = new InMemoryDocumentService();
            service.CreateDb("benchmark");

            var random = new Random(42);
            var vocabulary = GenerateVocabulary(200);
            int numDocs = 500;

            // Insert documents
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < numDocs; i++)
            {
                var tokens = vocabulary.OrderBy(_ => random.Next()).Take(random.Next(5, 15)).ToList();
                var doc = new Document
                {
                    Id = $"doc-{i}",
                    Content = string.Join(" ", tokens),
                    Data = new Dictionary<string, object>
                    {
                        ["title"] = $"Document {i}",
                        ["tokens"] = string.Join(", ", tokens.Take(3))
                    }
                };
                service.Post("benchmark", doc);
            }
            sw.Stop();
            _output.WriteLine($"Inserted {numDocs} docs in {sw.ElapsedMilliseconds} ms");

            // Get stats
            var stats = service.GetCompactIndexStats("benchmark");
            _output.WriteLine($"Compact index stats: {stats}");

            // Search with normal index
            var queries = vocabulary.Take(20).Select(t => t).ToArray();

            service.SetUseCompactIndex("benchmark", false);
            sw.Restart();
            int normalResults = 0;
            foreach (var q in queries)
            {
                normalResults += service.FullTextSearch("benchmark", q).Count();
            }
            sw.Stop();
            var normalTime = sw.ElapsedMilliseconds;

            // Search with compact index
            service.SetUseCompactIndex("benchmark", true);
            sw.Restart();
            int compactResults = 0;
            foreach (var q in queries)
            {
                compactResults += service.FullTextSearch("benchmark", q).Count();
            }
            sw.Stop();
            var compactTime = sw.ElapsedMilliseconds;

            _output.WriteLine($"=== DocumentService Search Comparison ===");
            _output.WriteLine($"Normal (HashSet):  {normalTime} ms, {normalResults} results");
            _output.WriteLine($"Compact (EF):      {compactTime} ms, {compactResults} results");

            // Verify correctness
            normalResults.Should().Be(compactResults, "both indexes should return same results");
        }

        #endregion

        #region Helpers

        private static List<string> GenerateVocabulary(int size)
        {
            return Enumerable.Range(0, size)
                .Select(i => $"word{i}")
                .ToList();
        }

        private static List<(string DocId, List<string> Tokens)> GenerateDocuments(
            int numDocs, int tokensPerDoc, List<string> vocabulary, Random random)
        {
            return Enumerable.Range(0, numDocs)
                .Select(i =>
                {
                    var tokens = vocabulary
                        .OrderBy(_ => random.Next())
                        .Take(tokensPerDoc)
                        .ToList();
                    return ($"doc-{i}", tokens);
                })
                .ToList();
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

        private static long EstimateHashSetMemory(Dictionary<string, HashSet<string>> index)
        {
            long bytes = 0;
            foreach (var kv in index)
            {
                // Key: string overhead (~40 bytes) + chars
                bytes += 40 + kv.Key.Length * 2;
                // HashSet overhead per entry: ~40 bytes (object header, hash, next pointer, value ref)
                // String value: ~40 bytes + chars
                foreach (var val in kv.Value)
                {
                    bytes += 40 + 40 + val.Length * 2;
                }
            }
            return bytes;
        }

        #endregion
    }
}

