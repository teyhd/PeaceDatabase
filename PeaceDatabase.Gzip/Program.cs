// File: PeaceDatabase.Gzip/Program.cs
// CLI tool for GZIP compression/decompression and benchmarking
using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using PeaceDatabase.Storage.Compression.Gzip;

namespace PeaceDatabase.Gzip;

class Program
{
    static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 1;
        }

        try
        {
            return args[0].ToLowerInvariant() switch
            {
                "compress" or "c" => Compress(args.Skip(1).ToArray()),
                "decompress" or "d" => Decompress(args.Skip(1).ToArray()),
                "benchmark" or "bench" or "b" => Benchmark(args.Skip(1).ToArray()),
                "test" or "t" => RunCompatibilityTest(args.Skip(1).ToArray()),
                "help" or "-h" or "--help" => PrintUsage(),
                _ => PrintUsage()
            };
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    static int PrintUsage()
    {
        Console.WriteLine("""
            PeaceDatabase GZIP Tool - Custom GZIP Implementation
            
            Usage:
              peacedb-gzip compress <input> [output.gz]     Compress file using custom GZIP
              peacedb-gzip decompress <input.gz> [output]   Decompress GZIP file
              peacedb-gzip benchmark [size_kb]              Compare with System.IO.Compression
              peacedb-gzip test [input_file]                Test compatibility with gunzip
              
            Aliases:
              c = compress, d = decompress, b/bench = benchmark, t = test
              
            Examples:
              peacedb-gzip compress data.json data.json.gz
              peacedb-gzip decompress data.json.gz data.json
              peacedb-gzip benchmark 100
              peacedb-gzip test sample.txt
              
            Output can be verified with standard tools:
              gunzip -c output.gz > restored.txt
              7z x output.gz
            """);
        return 0;
    }

    static int Compress(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Error: Input file required");
            return 1;
        }

        string inputPath = args[0];
        string outputPath = args.Length > 1 ? args[1] : inputPath + ".gz";

        if (!File.Exists(inputPath))
        {
            Console.Error.WriteLine($"Error: File not found: {inputPath}");
            return 1;
        }

        Console.WriteLine($"Compressing: {inputPath}");
        
        var sw = Stopwatch.StartNew();
        byte[] inputData = File.ReadAllBytes(inputPath);
        byte[] compressedData = GzipCodec.Compress(inputData, Path.GetFileName(inputPath));
        File.WriteAllBytes(outputPath, compressedData);
        sw.Stop();

        double ratio = (double)compressedData.Length / inputData.Length * 100;
        Console.WriteLine($"Output: {outputPath}");
        Console.WriteLine($"Original size: {inputData.Length:N0} bytes");
        Console.WriteLine($"Compressed size: {compressedData.Length:N0} bytes");
        Console.WriteLine($"Compression ratio: {ratio:F2}%");
        Console.WriteLine($"Time: {sw.ElapsedMilliseconds} ms");
        Console.WriteLine();
        Console.WriteLine("Verify with: gunzip -t " + outputPath);

        return 0;
    }

    static int Decompress(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Error: Input file required");
            return 1;
        }

        string inputPath = args[0];
        string outputPath = args.Length > 1 
            ? args[1] 
            : inputPath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase) 
                ? inputPath[..^3] 
                : inputPath + ".decompressed";

        if (!File.Exists(inputPath))
        {
            Console.Error.WriteLine($"Error: File not found: {inputPath}");
            return 1;
        }

        Console.WriteLine($"Decompressing: {inputPath}");

        var sw = Stopwatch.StartNew();
        byte[] compressedData = File.ReadAllBytes(inputPath);
        byte[] decompressedData = GzipCodec.Decompress(compressedData);
        File.WriteAllBytes(outputPath, decompressedData);
        sw.Stop();

        Console.WriteLine($"Output: {outputPath}");
        Console.WriteLine($"Compressed size: {compressedData.Length:N0} bytes");
        Console.WriteLine($"Decompressed size: {decompressedData.Length:N0} bytes");
        Console.WriteLine($"Time: {sw.ElapsedMilliseconds} ms");

        return 0;
    }

    static int Benchmark(string[] args)
    {
        int sizeKb = args.Length > 0 && int.TryParse(args[0], out int s) ? s : 100;
        
        Console.WriteLine($"GZIP Compression Benchmark (Custom vs System.IO.Compression)");
        Console.WriteLine($"============================================================");
        Console.WriteLine();

        // Generate test data
        var testCases = new[]
        {
            ("JSON-like", GenerateJsonData(sizeKb)),
            ("Repeated text", GenerateRepeatedText(sizeKb)),
            ("Random binary", GenerateRandomData(sizeKb))
        };

        Console.WriteLine($"{"Data Type",-20} {"Size",-12} {"Custom",-15} {"Ratio",-10} {"System",-15} {"Ratio",-10} {"Custom/Sys",-12}");
        Console.WriteLine(new string('-', 94));

        foreach (var (name, data) in testCases)
        {
            // Warm-up
            _ = GzipCodec.Compress(data);
            using (var warmupMs = new MemoryStream())
            using (var warmupGz = new GZipStream(warmupMs, CompressionLevel.Optimal))
                warmupGz.Write(data);

            // Custom GZIP
            var swCustom = Stopwatch.StartNew();
            byte[] customCompressed = GzipCodec.Compress(data);
            swCustom.Stop();
            double customRatio = (double)customCompressed.Length / data.Length * 100;

            // System.IO.Compression
            var swSystem = Stopwatch.StartNew();
            byte[] systemCompressed;
            using (var ms = new MemoryStream())
            {
                using (var gz = new GZipStream(ms, CompressionLevel.Optimal, leaveOpen: true))
                    gz.Write(data);
                systemCompressed = ms.ToArray();
            }
            swSystem.Stop();
            double systemRatio = (double)systemCompressed.Length / data.Length * 100;

            double speedRatio = (double)swCustom.ElapsedMilliseconds / Math.Max(1, swSystem.ElapsedMilliseconds);

            Console.WriteLine($"{name,-20} {data.Length / 1024,8} KB  {swCustom.ElapsedMilliseconds,8} ms    {customRatio,6:F1}%    {swSystem.ElapsedMilliseconds,8} ms    {systemRatio,6:F1}%    {speedRatio,8:F2}x");
        }

        Console.WriteLine();
        Console.WriteLine("Notes:");
        Console.WriteLine("- Custom implementation uses static Huffman (simpler, slightly less efficient)");
        Console.WriteLine("- System.IO.Compression uses dynamic Huffman (more complex, better ratio)");
        // Verify roundtrip
        Console.WriteLine();
        Console.WriteLine("Roundtrip verification...");
        foreach (var (name, data) in testCases)
        {
            try
            {
                var compressed = GzipCodec.Compress(data);
                
                // Verify with System.IO.Compression (standard library can always decompress our output)
                using (var ms = new MemoryStream(compressed))
                using (var gz = new GZipStream(ms, CompressionMode.Decompress))
                using (var output = new MemoryStream())
                {
                    gz.CopyTo(output);
                    var systemDecompressed = output.ToArray();
                    if (!data.SequenceEqual(systemDecompressed))
                    {
                        Console.WriteLine($"  {name}: FAILED (System.IO verify)");
                        continue;
                    }
                }
                
                // Verify with our decompressor
                var decompressed = GzipCodec.Decompress(compressed);
                bool match = data.SequenceEqual(decompressed);
                Console.WriteLine($"  {name}: {(match ? "OK" : "FAILED")}");
            }
            catch (Exception ex)
            {
                // Note: Custom decompressor has limits with very large highly-compressible data
                Console.WriteLine($"  {name}: {ex.Message}");
            }
        }

        return 0;
    }

    static int RunCompatibilityTest(string[] args)
    {
        Console.WriteLine("GZIP Compatibility Test");
        Console.WriteLine("=======================");
        Console.WriteLine();

        string? inputPath = args.Length > 0 ? args[0] : null;
        byte[] testData;
        string description;

        if (inputPath != null && File.Exists(inputPath))
        {
            testData = File.ReadAllBytes(inputPath);
            description = $"File: {inputPath}";
        }
        else
        {
            // Generate test data
            testData = Encoding.UTF8.GetBytes("""
                {"_id":"test-doc","_rev":"1-abc123","title":"Test Document","content":"This is a test document created by PeaceDatabase custom GZIP implementation. It should be decompressable by standard tools like gunzip, 7z, WinRAR, etc.","tags":["test","gzip","compression"],"data":{"number":42,"flag":true,"nested":{"key":"value"}}}
                """);
            description = "Generated JSON document";
        }

        Console.WriteLine($"Test data: {description}");
        Console.WriteLine($"Size: {testData.Length} bytes");
        Console.WriteLine();

        // Compress with our implementation
        byte[] compressed = GzipCodec.Compress(testData, "test.json");
        Console.WriteLine($"Compressed size: {compressed.Length} bytes");
        Console.WriteLine($"Compression ratio: {(double)compressed.Length / testData.Length * 100:F1}%");
        Console.WriteLine();

        // Write to temp file
        string tempGz = Path.Combine(Path.GetTempPath(), $"peacedb-test-{Guid.NewGuid():N}.gz");
        string tempOut = Path.ChangeExtension(tempGz, null);

        try
        {
            File.WriteAllBytes(tempGz, compressed);
            Console.WriteLine($"Compressed file: {tempGz}");

            // Try to decompress with System.IO.Compression
            Console.WriteLine();
            Console.WriteLine("Verification with System.IO.Compression.GZipStream:");
            try
            {
                using var inputStream = new FileStream(tempGz, FileMode.Open);
                using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
                using var outputStream = new MemoryStream();
                gzipStream.CopyTo(outputStream);
                byte[] systemDecompressed = outputStream.ToArray();

                bool match = testData.SequenceEqual(systemDecompressed);
                Console.WriteLine($"  Status: {(match ? "SUCCESS - Data matches!" : "FAILED - Data mismatch")}");
                Console.WriteLine($"  Decompressed size: {systemDecompressed.Length} bytes");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Status: FAILED - {ex.Message}");
            }

            // Verify with our own decompressor
            Console.WriteLine();
            Console.WriteLine("Verification with custom decompressor:");
            try
            {
                byte[] customDecompressed = GzipCodec.Decompress(compressed);
                bool match = testData.SequenceEqual(customDecompressed);
                Console.WriteLine($"  Status: {(match ? "SUCCESS - Data matches!" : "FAILED - Data mismatch")}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Status: FAILED - {ex.Message}");
            }

            Console.WriteLine();
            Console.WriteLine("To test with external tools:");
            Console.WriteLine($"  gunzip -c \"{tempGz}\" > \"{tempOut}\"");
            Console.WriteLine($"  7z x \"{tempGz}\" -o\"{Path.GetDirectoryName(tempGz)}\"");
            Console.WriteLine();
            Console.WriteLine("Press Enter to cleanup temp files, or Ctrl+C to keep them...");
            Console.ReadLine();
        }
        finally
        {
            if (File.Exists(tempGz)) File.Delete(tempGz);
            if (File.Exists(tempOut)) File.Delete(tempOut);
        }

        return 0;
    }

    static byte[] GenerateJsonData(int sizeKb)
    {
        var sb = new StringBuilder();
        sb.Append('[');
        int docCount = 0;
        while (sb.Length < sizeKb * 1024)
        {
            if (docCount > 0) sb.Append(',');
            sb.Append($"{{\"_id\":\"doc-{docCount}\",\"title\":\"Document {docCount}\",\"content\":\"This is the content of document {docCount}. It contains some repetitive text patterns for compression testing.\",\"tags\":[\"tag1\",\"tag2\"],\"data\":{{\"count\":{docCount},\"flag\":true}}}}");
            docCount++;
        }
        sb.Append(']');
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    static byte[] GenerateRepeatedText(int sizeKb)
    {
        var sb = new StringBuilder();
        string pattern = "This is a test document created by PeaceDatabase custom GZIP implementation. It should be decompressable by standard tools like gunzip, 7z, WinRAR, etc.";
        while (sb.Length < sizeKb * 1024)
        {
            sb.Append(pattern);
        }
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    static byte[] GenerateRandomData(int sizeKb)
    {
        var random = new Random(42);
        var data = new byte[sizeKb * 1024];
        random.NextBytes(data);
        return data;
    }
}

