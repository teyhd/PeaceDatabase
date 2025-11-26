// File: PeaceDatabase.Tests/GzipCodecTests.cs
using System.Text;
using FluentAssertions;
using PeaceDatabase.Storage.Compression.Deflate;
using PeaceDatabase.Storage.Compression.Gzip;
using Xunit;

namespace PeaceDatabase.Tests;

public class Crc32Tests
{
    [Fact]
    public void Compute_EmptyData_ReturnsZero()
    {
        var crc = Crc32.Compute(ReadOnlySpan<byte>.Empty);
        crc.Should().Be(0);
    }

    [Fact]
    public void Compute_KnownString_ReturnsExpectedCrc()
    {
        // "123456789" has CRC32 = 0xCBF43926
        var data = Encoding.ASCII.GetBytes("123456789");
        var crc = Crc32.Compute(data);
        crc.Should().Be(0xCBF43926);
    }

    [Fact]
    public void Compute_SingleByte_ReturnsCorrectValue()
    {
        var crc = Crc32.Compute(new byte[] { 0x00 });
        crc.Should().Be(0xD202EF8D);
    }

    [Fact]
    public void Update_StreamingComputation_MatchesSingleComputation()
    {
        var data1 = Encoding.ASCII.GetBytes("Hello, ");
        var data2 = Encoding.ASCII.GetBytes("World!");
        var fullData = Encoding.ASCII.GetBytes("Hello, World!");

        var crcFull = Crc32.Compute(fullData);
        
        var crcPartial = Crc32.Compute(data1);
        crcPartial = Crc32.Update(crcPartial, data2);

        crcPartial.Should().Be(crcFull);
    }
}

public class Lz77EncoderTests
{
    [Fact]
    public void Encode_EmptyInput_ReturnsEmptyTokens()
    {
        var encoder = new Lz77Encoder();
        var tokens = encoder.Encode(ReadOnlySpan<byte>.Empty);
        tokens.Should().BeEmpty();
    }

    [Fact]
    public void Encode_SingleByte_ReturnsOneLiteral()
    {
        var encoder = new Lz77Encoder();
        var tokens = encoder.Encode(new byte[] { 0x41 });
        
        tokens.Should().HaveCount(1);
        tokens[0].IsLiteral.Should().BeTrue();
        tokens[0].Literal.Should().Be(0x41);
    }

    [Fact]
    public void Encode_RepeatedSequence_FindsMatch()
    {
        var encoder = new Lz77Encoder();
        // "ABCDEFABCDEF" - second ABCDEF should match first
        var data = Encoding.ASCII.GetBytes("ABCDEFABCDEF");
        var tokens = encoder.Encode(data);

        // Should have literals for first ABCDEF, then a match for second
        tokens.Should().Contain(t => !t.IsLiteral && t.Length >= 3);
    }

    [Fact]
    public void Encode_NoRepetition_AllLiterals()
    {
        var encoder = new Lz77Encoder();
        var data = Encoding.ASCII.GetBytes("ABCDEFGH");
        var tokens = encoder.Encode(data);

        tokens.Should().OnlyContain(t => t.IsLiteral);
        tokens.Should().HaveCount(8);
    }
}

public class DeflateEncoderTests
{
    [Fact]
    public void Encode_EmptyInput_ProducesValidOutput()
    {
        var encoder = new DeflateEncoder();
        var result = encoder.Encode(ReadOnlySpan<byte>.Empty);
        
        result.Should().NotBeEmpty();
        // Should contain at least the block header and end-of-block marker
    }

    [Fact]
    public void Encode_SimpleText_ProducesCompressedOutput()
    {
        var encoder = new DeflateEncoder();
        var data = Encoding.ASCII.GetBytes("Hello, World!");
        var result = encoder.Encode(data);

        result.Should().NotBeEmpty();
    }

    [Fact]
    public void EncodeStored_SimpleText_ProducesStoredBlock()
    {
        var encoder = new DeflateEncoder();
        var data = Encoding.ASCII.GetBytes("Test data for stored block");
        var result = encoder.EncodeStored(data);

        result.Should().NotBeEmpty();
        // Stored block should be slightly larger than input
    }
}

public class DeflateRoundtripTests
{
    [Theory]
    [InlineData("")]
    [InlineData("A")]
    [InlineData("Hello")]
    [InlineData("Hello, World!")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")] // Highly compressible
    public void Roundtrip_VariousInputs_DecompressesToOriginal(string input)
    {
        var encoder = new DeflateEncoder();
        var decoder = new DeflateDecoder();
        var data = Encoding.UTF8.GetBytes(input);

        var compressed = encoder.Encode(data);
        var decompressed = decoder.Decode(compressed);

        Encoding.UTF8.GetString(decompressed).Should().Be(input);
    }

    [Fact]
    public void Roundtrip_BinaryData_DecompressesToOriginal()
    {
        var encoder = new DeflateEncoder();
        var decoder = new DeflateDecoder();
        var data = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x80, 0x7F };

        var compressed = encoder.Encode(data);
        var decompressed = decoder.Decode(compressed);

        decompressed.Should().Equal(data);
    }

    [Fact]
    public void Roundtrip_LargerText_DecompressesToOriginal()
    {
        var encoder = new DeflateEncoder();
        var decoder = new DeflateDecoder();
        
        // repetitive text that should compress well
        var sb = new StringBuilder();
        for (int i = 0; i < 100; i++)
        {
            sb.AppendLine($"Line {i}: This is a test line with some repeated content.");
        }
        var data = Encoding.UTF8.GetBytes(sb.ToString());

        var compressed = encoder.Encode(data);
        var decompressed = decoder.Decode(compressed);

        decompressed.Should().Equal(data);
    }
}

public class GzipCodecTests
{
    [Fact]
    public void IsGzipData_ValidGzip_ReturnsTrue()
    {
        var data = GzipCodec.Compress(Encoding.ASCII.GetBytes("Test"));
        GzipCodec.IsGzipData(data).Should().BeTrue();
    }

    [Fact]
    public void IsGzipData_InvalidData_ReturnsFalse()
    {
        GzipCodec.IsGzipData(new byte[] { 0x00, 0x00 }).Should().BeFalse();
        GzipCodec.IsGzipData(new byte[] { 0x1F }).Should().BeFalse();
        GzipCodec.IsGzipData(ReadOnlySpan<byte>.Empty).Should().BeFalse();
    }

    [Theory]
    [InlineData("")]
    [InlineData("A")]
    [InlineData("Hello, World!")]
    [InlineData("This is a test document for GZIP compression.")]
    [InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")]
    public void Roundtrip_VariousStrings_DecompressesToOriginal(string input)
    {
        var data = Encoding.UTF8.GetBytes(input);
        var compressed = GzipCodec.Compress(data);
        var decompressed = GzipCodec.Decompress(compressed);

        Encoding.UTF8.GetString(decompressed).Should().Be(input);
    }

    [Fact]
    public void Compress_WithFilename_IncludesFilenameInHeader()
    {
        var data = Encoding.ASCII.GetBytes("Test content");
        var compressed = GzipCodec.Compress(data, "test.txt");
        
        compressed.Should().NotBeEmpty();
        // Verify FNAME flag is set (byte 3, bit 3)
        (compressed[3] & 0x08).Should().NotBe(0);
        
        // Decompression should still work
        var decompressed = GzipCodec.Decompress(compressed);
        decompressed.Should().Equal(data);
    }

    [Fact]
    public void Compress_RepetitiveData_AchievesCompression()
    {
        var sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
        {
            sb.Append("ABCDEFGHIJ"); // Repetitive pattern
        }
        var data = Encoding.ASCII.GetBytes(sb.ToString());
        var compressed = GzipCodec.Compress(data);

        // repetitive data should compress significantly
        compressed.Length.Should().BeLessThan(data.Length / 2);
    }

    [Fact]
    public void Compress_RandomData_StillWorks()
    {
        var random = new Random(42);
        var data = new byte[1000];
        random.NextBytes(data);

        var compressed = GzipCodec.Compress(data);
        var decompressed = GzipCodec.Decompress(compressed);

        decompressed.Should().Equal(data);
    }

    [Fact]
    public void Decompress_CorruptedCrc_ThrowsException()
    {
        var data = Encoding.ASCII.GetBytes("Test data");
        var compressed = GzipCodec.Compress(data);

        // Corrupt the CRC (last 8 bytes contain CRC32 and ISIZE)
        compressed[^8] ^= 0xFF;

        var action = () => GzipCodec.Decompress(compressed);
        action.Should().Throw<InvalidDataException>().WithMessage("*CRC32*");
    }

    [Fact]
    public void Decompress_InvalidMagic_ThrowsException()
    {
        // Need at least 18 bytes for GZIP (10 header + 8 footer minimum)
        var data = new byte[] { 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

        var action = () => GzipCodec.Decompress(data);
        action.Should().Throw<InvalidDataException>().WithMessage("*magic*");
    }

    [Fact]
    public void Compress_JsonDocument_RoundtripsCorrectly()
    {
        var json = @"{""_id"":""doc-123"",""title"":""Test"",""tags"":[""a"",""b""]}";
        
        var data = Encoding.UTF8.GetBytes(json);
        var compressed = GzipCodec.Compress(data);
        var decompressed = GzipCodec.Decompress(compressed);

        Encoding.UTF8.GetString(decompressed).Should().Be(json);
    }
}

public class HuffmanTableTests
{
    [Theory]
    [InlineData(3, 257)]
    [InlineData(4, 258)]
    [InlineData(10, 264)]
    [InlineData(258, 285)]
    public void GetLengthSymbol_ValidLengths_ReturnsCorrectSymbol(int length, int expectedSymbol)
    {
        var symbol = HuffmanTable.GetLengthSymbol(length);
        symbol.Should().Be(expectedSymbol);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(2, 1)]
    [InlineData(5, 4)]
    [InlineData(100, 13)]  // distance 97-128 -> symbol 13
    [InlineData(32768, 29)]
    public void GetDistanceSymbol_ValidDistances_ReturnsCorrectSymbol(int distance, int expectedSymbol)
    {
        var symbol = HuffmanTable.GetDistanceSymbol(distance);
        symbol.Should().Be(expectedSymbol);
    }

    [Fact]
    public void StaticHuffman_LitLenCodes_HaveCorrectBitLengths()
    {
        for (int i = 0; i <= 143; i++)
        {
            var (_, bits) = StaticHuffman.GetLitLenCode(i);
            bits.Should().Be(8, $"literal {i} should have 8-bit code");
        }

        for (int i = 144; i <= 255; i++)
        {
            var (_, bits) = StaticHuffman.GetLitLenCode(i);
            bits.Should().Be(9, $"literal {i} should have 9-bit code");
        }

        for (int i = 256; i <= 279; i++)
        {
            var (_, bits) = StaticHuffman.GetLitLenCode(i);
            bits.Should().Be(7, $"symbol {i} should have 7-bit code");
        }

        for (int i = 280; i <= 287; i++)
        {
            var (_, bits) = StaticHuffman.GetLitLenCode(i);
            bits.Should().Be(8, $"symbol {i} should have 8-bit code");
        }
    }

    [Fact]
    public void StaticHuffman_DistCodes_AllHave5Bits()
    {
        for (int i = 0; i < 30; i++)
        {
            var (_, bits) = StaticHuffman.GetDistCode(i);
            bits.Should().Be(5);
        }
    }
}

