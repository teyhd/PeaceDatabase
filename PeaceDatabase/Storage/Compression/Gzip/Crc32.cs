// File: Storage/Compression/Gzip/Crc32.cs
namespace PeaceDatabase.Storage.Compression.Gzip;

/// <summary>
/// CRC32 implementation per RFC 1952 (GZIP) using polynomial 0xEDB88320.
/// Table-driven for performance.
/// </summary>
public static class Crc32
{
    private const uint Polynomial = 0xEDB88320;
    private static readonly uint[] Table = GenerateTable();

    private static uint[] GenerateTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
            {
                crc = (crc & 1) != 0 
                    ? (crc >> 1) ^ Polynomial 
                    : crc >> 1;
            }
            table[i] = crc;
        }
        return table;
    }

    /// <summary>
    /// Compute CRC32 checksum for the given data.
    /// </summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }

    /// <summary>
    /// Update running CRC32 with additional data (for streaming).
    /// </summary>
    public static uint Update(uint crc, ReadOnlySpan<byte> data)
    {
        crc ^= 0xFFFFFFFF;
        foreach (byte b in data)
        {
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }
}

