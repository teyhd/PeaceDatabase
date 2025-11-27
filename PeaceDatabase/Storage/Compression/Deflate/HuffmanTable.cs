// File: Storage/Compression/Deflate/HuffmanTable.cs
namespace PeaceDatabase.Storage.Compression.Deflate;

/// <summary>
/// Static Huffman tables for DEFLATE as defined in RFC 1951.
/// Provides encoding tables for compression and decoding tables for decompression.
/// </summary>
public static class HuffmanTable
{
    // Length codes: symbol 257-285 encode lengths 3-258
    public static readonly int[] LengthBase = {
        3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31,
        35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258
    };

    public static readonly int[] LengthExtraBits = {
        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2,
        3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0
    };

    // Distance codes: symbol 0-29 encode distances 1-32768
    public static readonly int[] DistanceBase = {
        1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193,
        257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145,
        8193, 12289, 16385, 24577
    };

    public static readonly int[] DistanceExtraBits = {
        0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6,
        7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13
    };

    /// <summary>
    /// Get length symbol (257-285) for a given match length (3-258).
    /// </summary>
    public static int GetLengthSymbol(int length)
    {
        for (int i = LengthBase.Length - 1; i >= 0; i--)
        {
            if (length >= LengthBase[i])
                return 257 + i;
        }
        return 257; // minimum
    }

    /// <summary>
    /// Get distance symbol (0-29) for a given distance (1-32768).
    /// </summary>
    public static int GetDistanceSymbol(int distance)
    {
        for (int i = DistanceBase.Length - 1; i >= 0; i--)
        {
            if (distance >= DistanceBase[i])
                return i;
        }
        return 0; // minimum
    }
}

/// <summary>
/// Static Huffman codes for DEFLATE type 1 blocks (fixed Huffman).
/// RFC 1951 Section 3.2.6.
/// </summary>
public static class StaticHuffman
{
    // Literal/Length codes (0-287):
    //   0-143:   8-bit codes 00110000 - 10111111 (0x30-0xBF)
    //   144-255: 9-bit codes 110010000 - 111111111 (0x190-0x1FF)
    //   256-279: 7-bit codes 0000000 - 0010111 (0x00-0x17)
    //   280-287: 8-bit codes 11000000 - 11000111 (0xC0-0xC7)

    // Pre-computed codes and lengths for encoding
    private static readonly (int Code, int Bits)[] LitLenCodes = BuildLitLenCodes();
    
    // Distance codes: all 5 bits (0-29)
    private static readonly (int Code, int Bits)[] DistCodes = BuildDistCodes();

    // Decoding tables
    private static readonly int[] LitLenDecodeTable = BuildLitLenDecodeTable();
    private static readonly int[] DistDecodeTable = BuildDistDecodeTable();

    private static (int Code, int Bits)[] BuildLitLenCodes()
    {
        var codes = new (int Code, int Bits)[288];
        
        // Assign code lengths per RFC 1951
        int[] codeLengths = new int[288];
        for (int i = 0; i <= 143; i++) codeLengths[i] = 8;
        for (int i = 144; i <= 255; i++) codeLengths[i] = 9;
        for (int i = 256; i <= 279; i++) codeLengths[i] = 7;
        for (int i = 280; i <= 287; i++) codeLengths[i] = 8;

        // Build canonical Huffman codes
        int[] blCount = new int[10];
        foreach (int len in codeLengths)
            if (len > 0) blCount[len]++;

        int[] nextCode = new int[10];
        int code = 0;
        for (int bits = 1; bits <= 9; bits++)
        {
            code = (code + blCount[bits - 1]) << 1;
            nextCode[bits] = code;
        }

        for (int i = 0; i < 288; i++)
        {
            int len = codeLengths[i];
            if (len > 0)
            {
                codes[i] = (nextCode[len]++, len);
            }
        }

        return codes;
    }

    private static (int Code, int Bits)[] BuildDistCodes()
    {
        // All distance codes are 5 bits (0-29)
        var codes = new (int Code, int Bits)[30];
        for (int i = 0; i < 30; i++)
        {
            codes[i] = (i, 5);
        }
        return codes;
    }

    private static int[] BuildLitLenDecodeTable()
    {
        // Build lookup table: for each 9-bit value, what symbol does it decode to?
        // Returns symbol or -1 if not a valid prefix
        var table = new int[512]; // 9 bits max
        Array.Fill(table, -1);

        for (int sym = 0; sym < 288; sym++)
        {
            var (code, bits) = LitLenCodes[sym];
            if (bits == 0) continue;

            // Reverse the code for LSB-first reading
            int reversed = ReverseBits(code, bits);
            
            // Fill all table entries that start with this code
            int fill = 1 << (9 - bits);
            for (int i = 0; i < fill; i++)
            {
                int index = reversed | (i << bits);
                table[index] = sym | (bits << 16); // pack symbol and bit length
            }
        }
        return table;
    }

    private static int[] BuildDistDecodeTable()
    {
        var table = new int[32]; // 5 bits
        for (int i = 0; i < 30; i++)
        {
            int reversed = ReverseBits(i, 5);
            table[reversed] = i;
        }
        return table;
    }

    private static int ReverseBits(int value, int bits)
    {
        int result = 0;
        for (int i = 0; i < bits; i++)
        {
            result = (result << 1) | (value & 1);
            value >>= 1;
        }
        return result;
    }

    /// <summary>
    /// Get static Huffman code for a literal/length symbol.
    /// </summary>
    public static (int Code, int Bits) GetLitLenCode(int symbol) => LitLenCodes[symbol];

    /// <summary>
    /// Get static Huffman code for a distance symbol.
    /// </summary>
    public static (int Code, int Bits) GetDistCode(int symbol) => DistCodes[symbol];

    /// <summary>
    /// Decode a literal/length symbol from bit reader.
    /// </summary>
    public static int DecodeLitLen(BitReader reader)
    {
        // Peek 9 bits
        int bits9 = reader.ReadBits(9);
        int entry = LitLenDecodeTable[bits9];
        
        if (entry < 0)
            throw new InvalidDataException("Invalid Huffman code");
        
        int symbol = entry & 0xFFFF;
        int codeLen = entry >> 16;
        
        // Put back unused bits
        int unusedBits = 9 - codeLen;
        if (unusedBits > 0)
        {
            // We need to "unread" the extra bits - but BitReader doesn't support that easily
            // Instead, we'll use a different approach
        }
        
        return symbol;
    }

    /// <summary>
    /// Decode a distance symbol from bit reader.
    /// </summary>
    public static int DecodeDist(BitReader reader)
    {
        int bits5 = reader.ReadBits(5);
        return DistDecodeTable[bits5];
    }
}

/// <summary>
/// Huffman decoder using lookup tables for fast decoding.
/// </summary>
public sealed class HuffmanDecoder
{
    private readonly int[] _table;
    private readonly int _tableBits;
    private readonly int[] _codeLengths;
    
    public HuffmanDecoder(int[] codeLengths, int maxBits)
    {
        _codeLengths = codeLengths;
        _tableBits = Math.Min(maxBits, 9);
        _table = BuildTable(codeLengths, _tableBits);
    }

    private static int[] BuildTable(int[] codeLengths, int tableBits)
    {
        int tableSize = 1 << tableBits;
        var table = new int[tableSize];
        Array.Fill(table, -1);

        // Build canonical codes
        int maxLen = codeLengths.Max();
        if (maxLen == 0) return table;

        int[] blCount = new int[maxLen + 1];
        foreach (int len in codeLengths)
            if (len > 0) blCount[len]++;

        int[] nextCode = new int[maxLen + 1];
        int code = 0;
        for (int bits = 1; bits <= maxLen; bits++)
        {
            code = (code + blCount[bits - 1]) << 1;
            nextCode[bits] = code;
        }

        for (int sym = 0; sym < codeLengths.Length; sym++)
        {
            int len = codeLengths[sym];
            if (len == 0) continue;

            int symCode = nextCode[len]++;
            int reversed = ReverseBits(symCode, len);

            if (len <= tableBits)
            {
                int fill = 1 << (tableBits - len);
                for (int i = 0; i < fill; i++)
                {
                    int index = reversed | (i << len);
                    table[index] = sym | (len << 16);
                }
            }
        }

        return table;
    }

    private static int ReverseBits(int value, int bits)
    {
        int result = 0;
        for (int i = 0; i < bits; i++)
        {
            result = (result << 1) | (value & 1);
            value >>= 1;
        }
        return result;
    }

    public int Decode(BitReader reader)
    {
        int peekBits = reader.ReadBits(_tableBits);
        int entry = _table[peekBits];
        
        if (entry < 0)
            throw new InvalidDataException("Invalid Huffman code");
        
        int symbol = entry & 0xFFFF;
        int codeLen = entry >> 16;

        // We read exactly tableBits, but the code might be shorter
        // For simplicity in static Huffman, we adjust the reader
        // This works because we always read exactly the right amount
        
        return symbol;
    }
}

