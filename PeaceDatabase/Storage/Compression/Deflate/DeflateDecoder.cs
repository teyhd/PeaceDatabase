// File: Storage/Compression/Deflate/DeflateDecoder.cs
namespace PeaceDatabase.Storage.Compression.Deflate;

/// <summary>
/// DEFLATE decoder per RFC 1951.
/// Supports all block types: stored (0), static Huffman (1), and dynamic Huffman (2).
/// </summary>
public sealed class DeflateDecoder
{
    private const int EndOfBlock = 256;
    private const int MaxWindowSize = 32768;

    // Pre-built static Huffman decoder tables for fast decoding
    // Entry format: symbol | (codeLength << 16)
    private static readonly int[] StaticLitLenTable = BuildStaticLitLenTable();
    private static readonly int[] StaticDistTable = BuildStaticDistTable();

    /// <summary>
    /// Decompress DEFLATE data.
    /// </summary>
    public byte[] Decode(byte[] compressedData)
    {
        var reader = new BitReader(compressedData);
        using var output = new MemoryStream();
        var window = new byte[MaxWindowSize];
        int windowPos = 0;

        bool isFinal = false;
        while (!isFinal)
        {
            isFinal = reader.ReadBits(1) == 1;
            int blockType = reader.ReadBits(2);

            switch (blockType)
            {
                case 0:
                    DecodeStoredBlock(reader, output, window, ref windowPos);
                    break;
                case 1:
                    DecodeStaticHuffmanBlock(reader, output, window, ref windowPos);
                    break;
                case 2:
                    DecodeDynamicHuffmanBlock(reader, output, window, ref windowPos);
                    break;
                default:
                    throw new InvalidDataException($"Invalid DEFLATE block type: {blockType}");
            }
        }

        return output.ToArray();
    }

    private void DecodeStoredBlock(BitReader reader, MemoryStream output, byte[] window, ref int windowPos)
    {
        reader.AlignToByte();

        ushort len = reader.ReadUInt16LE();
        ushort nlen = reader.ReadUInt16LE();

        if ((ushort)~len != nlen)
            throw new InvalidDataException("Invalid stored block length");

        for (int i = 0; i < len; i++)
        {
            byte b = reader.ReadByte();
            output.WriteByte(b);
            window[windowPos] = b;
            windowPos = (windowPos + 1) & (MaxWindowSize - 1);
        }
    }

    private void DecodeStaticHuffmanBlock(BitReader reader, MemoryStream output, byte[] window, ref int windowPos)
    {
        while (true)
        {
            int symbol = DecodeStaticLitLen(reader);

            if (symbol < 256)
            {
                // Literal byte
                byte b = (byte)symbol;
                output.WriteByte(b);
                window[windowPos] = b;
                windowPos = (windowPos + 1) & (MaxWindowSize - 1);
            }
            else if (symbol == EndOfBlock)
            {
                break;
            }
            else
            {
                // Length-distance pair
                int length = DecodeLengthValue(reader, symbol);
                int distSymbol = DecodeStaticDistance(reader);
                int distance = DecodeDistanceValue(reader, distSymbol);

                // Copy from window
                CopyFromWindow(output, window, ref windowPos, length, distance);
            }
        }
    }

    private void DecodeDynamicHuffmanBlock(BitReader reader, MemoryStream output, byte[] window, ref int windowPos)
    {
        // Read Huffman table definitions
        int hlit = reader.ReadBits(5) + 257;   // Number of literal/length codes
        int hdist = reader.ReadBits(5) + 1;    // Number of distance codes
        int hclen = reader.ReadBits(4) + 4;    // Number of code length codes

        // Code length code lengths order
        int[] codeLengthOrder = { 16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15 };
        int[] codeLengthCodeLengths = new int[19];

        for (int i = 0; i < hclen; i++)
        {
            codeLengthCodeLengths[codeLengthOrder[i]] = reader.ReadBits(3);
        }

        // Build code length decoder
        var codeLengthTable = BuildHuffmanTable(codeLengthCodeLengths, 7);

        // Read literal/length and distance code lengths
        int[] allCodeLengths = new int[hlit + hdist];
        int index = 0;

        while (index < allCodeLengths.Length)
        {
            int sym = DecodeWithTable(reader, codeLengthTable, 7);

            if (sym < 16)
            {
                allCodeLengths[index++] = sym;
            }
            else if (sym == 16)
            {
                int repeat = reader.ReadBits(2) + 3;
                int value = index > 0 ? allCodeLengths[index - 1] : 0;
                for (int i = 0; i < repeat && index < allCodeLengths.Length; i++)
                    allCodeLengths[index++] = value;
            }
            else if (sym == 17)
            {
                int repeat = reader.ReadBits(3) + 3;
                for (int i = 0; i < repeat && index < allCodeLengths.Length; i++)
                    allCodeLengths[index++] = 0;
            }
            else if (sym == 18)
            {
                int repeat = reader.ReadBits(7) + 11;
                for (int i = 0; i < repeat && index < allCodeLengths.Length; i++)
                    allCodeLengths[index++] = 0;
            }
        }

        // Build decoders
        var litLenCodeLengths = allCodeLengths.Take(hlit).ToArray();
        var distCodeLengths = allCodeLengths.Skip(hlit).Take(hdist).ToArray();

        var litLenTable = BuildHuffmanTable(litLenCodeLengths, 9);
        var distTable = BuildHuffmanTable(distCodeLengths, 9);

        // Decode block
        while (true)
        {
            int symbol = DecodeWithTable(reader, litLenTable, 9);

            if (symbol < 256)
            {
                byte b = (byte)symbol;
                output.WriteByte(b);
                window[windowPos] = b;
                windowPos = (windowPos + 1) & (MaxWindowSize - 1);
            }
            else if (symbol == EndOfBlock)
            {
                break;
            }
            else
            {
                int length = DecodeLengthValue(reader, symbol);
                int distSymbol = DecodeWithTable(reader, distTable, 9);
                int distance = DecodeDistanceValue(reader, distSymbol);

                CopyFromWindow(output, window, ref windowPos, length, distance);
            }
        }
    }

    /// <summary>
    /// Decode static Huffman literal/length symbol using peek/consume.
    /// </summary>
    private int DecodeStaticLitLen(BitReader reader)
    {
        // Peek 9 bits (maximum static Huffman code length)
        int bits = reader.PeekBits(9);
        int entry = StaticLitLenTable[bits];
        
        if (entry < 0)
            throw new InvalidDataException("Invalid static Huffman code");
        
        int symbol = entry & 0xFFFF;
        int codeLen = entry >> 16;
        
        // Consume only the bits we actually used
        reader.ConsumeBits(codeLen);
        
        return symbol;
    }

    private int DecodeStaticDistance(BitReader reader)
    {
        // Static distance codes are 5 bits
        int bits = reader.PeekBits(5);
        int entry = StaticDistTable[bits];
        int symbol = entry & 0xFFFF;
        reader.ConsumeBits(5);
        return symbol;
    }

    private int DecodeWithTable(BitReader reader, int[] table, int tableBits)
    {
        int bits = reader.PeekBits(tableBits);
        int entry = table[bits];
        
        if (entry < 0)
            throw new InvalidDataException("Invalid Huffman code");
        
        int symbol = entry & 0xFFFF;
        int codeLen = entry >> 16;
        reader.ConsumeBits(codeLen);
        
        return symbol;
    }

    private static int[] BuildStaticLitLenTable()
    {
        // Build lookup table for 9-bit input
        // Entry format: symbol | (codeLength << 16)
        var table = new int[512];
        Array.Fill(table, -1);

        // Build canonical codes for static Huffman per RFC 1951
        int[] codeLengths = new int[288];
        for (int i = 0; i <= 143; i++) codeLengths[i] = 8;
        for (int i = 144; i <= 255; i++) codeLengths[i] = 9;
        for (int i = 256; i <= 279; i++) codeLengths[i] = 7;
        for (int i = 280; i <= 287; i++) codeLengths[i] = 8;

        return BuildHuffmanTable(codeLengths, 9);
    }

    private static int[] BuildStaticDistTable()
    {
        // Static distance codes are 5 bits (0-29)
        // All codes have length 5
        var table = new int[32];
        
        for (int sym = 0; sym < 30; sym++)
        {
            // Reverse the 5-bit code for LSB-first reading
            int reversed = ReverseBits(sym, 5);
            table[reversed] = sym | (5 << 16);
        }
        
        return table;
    }

    private static int[] BuildHuffmanTable(int[] codeLengths, int tableBits)
    {
        int tableSize = 1 << tableBits;
        var table = new int[tableSize];
        Array.Fill(table, -1);

        int maxLen = 0;
        foreach (int len in codeLengths)
            if (len > maxLen) maxLen = len;
        
        if (maxLen == 0) return table;

        // Count codes of each length
        int[] blCount = new int[maxLen + 1];
        foreach (int len in codeLengths)
            if (len > 0) blCount[len]++;

        // Generate starting code for each length
        int[] nextCode = new int[maxLen + 1];
        int code = 0;
        for (int bits = 1; bits <= maxLen; bits++)
        {
            code = (code + blCount[bits - 1]) << 1;
            nextCode[bits] = code;
        }

        // Assign codes and fill table
        for (int sym = 0; sym < codeLengths.Length; sym++)
        {
            int len = codeLengths[sym];
            if (len == 0) continue;

            int symCode = nextCode[len]++;
            int reversed = ReverseBits(symCode, len);

            if (len <= tableBits)
            {
                // Fill all table entries that have this code as prefix
                int fillCount = 1 << (tableBits - len);
                for (int i = 0; i < fillCount; i++)
                {
                    int tableIndex = reversed | (i << len);
                    table[tableIndex] = sym | (len << 16);
                }
            }
        }

        return table;
    }

    private int DecodeLengthValue(BitReader reader, int symbol)
    {
        int index = symbol - 257;
        if (index < 0 || index >= HuffmanTable.LengthBase.Length)
            throw new InvalidDataException($"Invalid length symbol: {symbol}");

        int baseLen = HuffmanTable.LengthBase[index];
        int extraBits = HuffmanTable.LengthExtraBits[index];

        if (extraBits > 0)
            return baseLen + reader.ReadBits(extraBits);
        return baseLen;
    }

    private int DecodeDistanceValue(BitReader reader, int symbol)
    {
        if (symbol < 0 || symbol >= HuffmanTable.DistanceBase.Length)
            throw new InvalidDataException($"Invalid distance symbol: {symbol}");

        int baseDist = HuffmanTable.DistanceBase[symbol];
        int extraBits = HuffmanTable.DistanceExtraBits[symbol];

        if (extraBits > 0)
            return baseDist + reader.ReadBits(extraBits);
        return baseDist;
    }

    private void CopyFromWindow(MemoryStream output, byte[] window, ref int windowPos, int length, int distance)
    {
        int srcPos = (windowPos - distance) & (MaxWindowSize - 1);

        for (int i = 0; i < length; i++)
        {
            byte b = window[srcPos];
            output.WriteByte(b);
            window[windowPos] = b;
            srcPos = (srcPos + 1) & (MaxWindowSize - 1);
            windowPos = (windowPos + 1) & (MaxWindowSize - 1);
        }
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
}
