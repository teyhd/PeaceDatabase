// File: Storage/Compression/Gzip/GzipCodec.cs
using System.Buffers.Binary;
using PeaceDatabase.Storage.Compression.Deflate;

namespace PeaceDatabase.Storage.Compression.Gzip;

/// <summary>
/// GZIP codec per RFC 1952.
/// Produces standard .gz files compatible with gunzip, 7z, and other tools.
/// </summary>
public static class GzipCodec
{
    // GZIP magic number
    private const byte Magic1 = 0x1F;
    private const byte Magic2 = 0x8B;
    
    // Compression method
    private const byte MethodDeflate = 8;
    
    // Flags
    private const byte FlagNone = 0;
    private const byte FlagFTEXT = 1;
    private const byte FlagFHCRC = 2;
    private const byte FlagFEXTRA = 4;
    private const byte FlagFNAME = 8;
    private const byte FlagFCOMMENT = 16;
    
    // OS identifiers
    private const byte OsFat = 0;
    private const byte OsUnix = 3;
    private const byte OsNtfs = 11;
    private const byte OsUnknown = 255;

    /// <summary>
    /// Compress data to GZIP format.
    /// </summary>
    public static byte[] Compress(ReadOnlySpan<byte> input)
    {
        using var ms = new MemoryStream();

        // Write GZIP header (10 bytes minimum)
        WriteHeader(ms);

        // Compress with DEFLATE
        var deflateEncoder = new DeflateEncoder();
        byte[] deflateData = deflateEncoder.Encode(input);
        ms.Write(deflateData);

        // Write footer: CRC32 + ISIZE
        WriteFooter(ms, input);

        return ms.ToArray();
    }

    /// <summary>
    /// Compress data to GZIP format with optional filename.
    /// </summary>
    public static byte[] Compress(ReadOnlySpan<byte> input, string? fileName)
    {
        using var ms = new MemoryStream();

        // Write GZIP header with filename
        WriteHeader(ms, fileName);

        // Compress with DEFLATE
        var deflateEncoder = new DeflateEncoder();
        byte[] deflateData = deflateEncoder.Encode(input);
        ms.Write(deflateData);

        // Write footer: CRC32 + ISIZE
        WriteFooter(ms, input);

        return ms.ToArray();
    }

    /// <summary>
    /// Decompress GZIP data.
    /// </summary>
    public static byte[] Decompress(byte[] gzipData)
    {
        if (gzipData.Length < 18)
            throw new InvalidDataException("GZIP data too short");

        int offset = 0;

        // Verify magic number
        if (gzipData[offset++] != Magic1 || gzipData[offset++] != Magic2)
            throw new InvalidDataException("Invalid GZIP magic number");

        // Compression method
        byte method = gzipData[offset++];
        if (method != MethodDeflate)
            throw new InvalidDataException($"Unsupported compression method: {method}");

        // Flags
        byte flags = gzipData[offset++];

        // MTIME (4 bytes) - skip
        offset += 4;

        // XFL (1 byte) - skip
        offset++;

        // OS (1 byte) - skip
        offset++;

        // Extra field (if FEXTRA)
        if ((flags & FlagFEXTRA) != 0)
        {
            if (offset + 2 > gzipData.Length)
                throw new InvalidDataException("Invalid GZIP extra field");
            int xlen = gzipData[offset] | (gzipData[offset + 1] << 8);
            offset += 2 + xlen;
        }

        // Original filename (if FNAME) - null-terminated
        if ((flags & FlagFNAME) != 0)
        {
            while (offset < gzipData.Length && gzipData[offset] != 0)
                offset++;
            offset++; // Skip null terminator
        }

        // Comment (if FCOMMENT) - null-terminated
        if ((flags & FlagFCOMMENT) != 0)
        {
            while (offset < gzipData.Length && gzipData[offset] != 0)
                offset++;
            offset++; // Skip null terminator
        }

        // Header CRC16 (if FHCRC)
        if ((flags & FlagFHCRC) != 0)
        {
            offset += 2;
        }

        // Extract DEFLATE data (everything except last 8 bytes for footer)
        if (offset + 8 > gzipData.Length)
            throw new InvalidDataException("Invalid GZIP data");

        int deflateLen = gzipData.Length - offset - 8;
        byte[] deflateData = new byte[deflateLen];
        Array.Copy(gzipData, offset, deflateData, 0, deflateLen);

        // Read footer
        int footerOffset = gzipData.Length - 8;
        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(gzipData.AsSpan(footerOffset, 4));
        uint expectedSize = BinaryPrimitives.ReadUInt32LittleEndian(gzipData.AsSpan(footerOffset + 4, 4));

        // Decompress
        var deflateDecoder = new DeflateDecoder();
        byte[] decompressed = deflateDecoder.Decode(deflateData);

        // Verify CRC32
        uint actualCrc = Crc32.Compute(decompressed);
        if (actualCrc != expectedCrc)
            throw new InvalidDataException($"CRC32 mismatch: expected {expectedCrc:X8}, got {actualCrc:X8}");

        // Verify size (mod 2^32)
        uint actualSize = (uint)(decompressed.Length & 0xFFFFFFFF);
        if (actualSize != expectedSize)
            throw new InvalidDataException($"Size mismatch: expected {expectedSize}, got {actualSize}");

        return decompressed;
    }

    private static void WriteHeader(MemoryStream ms, string? fileName = null)
    {
        // Magic number
        ms.WriteByte(Magic1);
        ms.WriteByte(Magic2);

        // Compression method (8 = deflate)
        ms.WriteByte(MethodDeflate);

        // Flags
        byte flags = FlagNone;
        if (!string.IsNullOrEmpty(fileName))
            flags |= FlagFNAME;
        ms.WriteByte(flags);

        // MTIME (modification time as Unix timestamp)
        uint mtime = (uint)(DateTimeOffset.UtcNow.ToUnixTimeSeconds() & 0xFFFFFFFF);
        Span<byte> mtimeBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(mtimeBytes, mtime);
        ms.Write(mtimeBytes);

        // XFL (extra flags): 0 = normal
        ms.WriteByte(0);

        // OS (operating system)
        ms.WriteByte(OsUnknown);

        // Filename (if FNAME flag set)
        if (!string.IsNullOrEmpty(fileName))
        {
            byte[] nameBytes = System.Text.Encoding.Latin1.GetBytes(fileName);
            ms.Write(nameBytes);
            ms.WriteByte(0); // Null terminator
        }
    }

    private static void WriteFooter(MemoryStream ms, ReadOnlySpan<byte> originalData)
    {
        // CRC32
        uint crc = Crc32.Compute(originalData);
        Span<byte> crcBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(crcBytes, crc);
        ms.Write(crcBytes);

        // ISIZE (original size mod 2^32)
        uint isize = (uint)(originalData.Length & 0xFFFFFFFF);
        Span<byte> isizeBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(isizeBytes, isize);
        ms.Write(isizeBytes);
    }

    /// <summary>
    /// Check if data starts with GZIP magic number.
    /// </summary>
    public static bool IsGzipData(ReadOnlySpan<byte> data)
    {
        return data.Length >= 2 && data[0] == Magic1 && data[1] == Magic2;
    }
}

