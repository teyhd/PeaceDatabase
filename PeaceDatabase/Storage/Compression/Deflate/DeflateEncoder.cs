// File: Storage/Compression/Deflate/DeflateEncoder.cs
namespace PeaceDatabase.Storage.Compression.Deflate;

/// <summary>
/// DEFLATE encoder per RFC 1951.
/// Uses static Huffman codes (block type 1) for simplicity and good compatibility.
/// </summary>
public sealed class DeflateEncoder
{
    private const int MaxBlockSize = 65535;    // Max size for stored blocks
    private const int EndOfBlock = 256;        // End of block symbol

    private readonly Lz77Encoder _lz77;

    public DeflateEncoder()
    {
        _lz77 = new Lz77Encoder();
    }

    /// <summary>
    /// Compress data using DEFLATE algorithm.
    /// </summary>
    public byte[] Encode(ReadOnlySpan<byte> input)
    {
        using var ms = new MemoryStream();
        var writer = new BitWriter(ms);

        if (input.Length == 0)
        {
            // Empty input: write final empty block
            WriteStaticBlock(writer, Array.Empty<Lz77Token>(), isFinal: true);
        }
        else
        {
            // Encode with LZ77
            var tokens = _lz77.Encode(input);
            
            // Write as single static Huffman block
            WriteStaticBlock(writer, tokens, isFinal: true);
        }

        writer.Flush();
        return ms.ToArray();
    }

    /// <summary>
    /// Compress data using stored blocks (no compression, for testing).
    /// </summary>
    public byte[] EncodeStored(ReadOnlySpan<byte> input)
    {
        using var ms = new MemoryStream();
        var writer = new BitWriter(ms);

        int offset = 0;
        while (offset < input.Length)
        {
            int blockSize = Math.Min(MaxBlockSize, input.Length - offset);
            bool isFinal = (offset + blockSize >= input.Length);

            WriteStoredBlock(writer, input.Slice(offset, blockSize), isFinal);
            offset += blockSize;
        }

        if (input.Length == 0)
        {
            // Empty input: write final empty stored block
            WriteStoredBlock(writer, ReadOnlySpan<byte>.Empty, isFinal: true);
        }

        writer.Flush();
        return ms.ToArray();
    }

    private void WriteStoredBlock(BitWriter writer, ReadOnlySpan<byte> data, bool isFinal)
    {
        // Block header: BFINAL (1 bit) + BTYPE (2 bits = 00 for stored)
        writer.WriteBits(isFinal ? 1 : 0, 1);  // BFINAL
        writer.WriteBits(0, 2);                 // BTYPE = 00 (stored)

        // Align to byte boundary
        writer.AlignToByte();

        // LEN and NLEN
        int len = data.Length;
        int nlen = ~len & 0xFFFF;

        writer.WriteBits(len & 0xFF, 8);
        writer.WriteBits((len >> 8) & 0xFF, 8);
        writer.WriteBits(nlen & 0xFF, 8);
        writer.WriteBits((nlen >> 8) & 0xFF, 8);

        // Raw data
        foreach (byte b in data)
            writer.WriteBits(b, 8);
    }

    private void WriteStaticBlock(BitWriter writer, IReadOnlyList<Lz77Token> tokens, bool isFinal)
    {
        // Block header: BFINAL (1 bit) + BTYPE (2 bits = 01 for static Huffman)
        writer.WriteBits(isFinal ? 1 : 0, 1);  // BFINAL
        writer.WriteBits(1, 2);                 // BTYPE = 01 (static Huffman)

        // Write tokens using static Huffman codes
        foreach (var token in tokens)
        {
            if (token.IsLiteral)
            {
                WriteLiteral(writer, token.Literal);
            }
            else
            {
                WriteLengthDistance(writer, token.Length, token.Distance);
            }
        }

        // End of block symbol (256)
        WriteEndOfBlock(writer);
    }

    private void WriteLiteral(BitWriter writer, byte literal)
    {
        var (code, bits) = StaticHuffman.GetLitLenCode(literal);
        writer.WriteBitsReversed(code, bits);
    }

    private void WriteEndOfBlock(BitWriter writer)
    {
        var (code, bits) = StaticHuffman.GetLitLenCode(EndOfBlock);
        writer.WriteBitsReversed(code, bits);
    }

    private void WriteLengthDistance(BitWriter writer, int length, int distance)
    {
        // Encode length
        int lengthSymbol = HuffmanTable.GetLengthSymbol(length);
        var (lengthCode, lengthBits) = StaticHuffman.GetLitLenCode(lengthSymbol);
        writer.WriteBitsReversed(lengthCode, lengthBits);

        // Extra bits for length
        int lengthIndex = lengthSymbol - 257;
        int extraLengthBits = HuffmanTable.LengthExtraBits[lengthIndex];
        if (extraLengthBits > 0)
        {
            int extraValue = length - HuffmanTable.LengthBase[lengthIndex];
            writer.WriteBits(extraValue, extraLengthBits);
        }

        // Encode distance
        int distSymbol = HuffmanTable.GetDistanceSymbol(distance);
        var (distCode, distBits) = StaticHuffman.GetDistCode(distSymbol);
        writer.WriteBitsReversed(distCode, distBits);

        // Extra bits for distance
        int extraDistBits = HuffmanTable.DistanceExtraBits[distSymbol];
        if (extraDistBits > 0)
        {
            int extraValue = distance - HuffmanTable.DistanceBase[distSymbol];
            writer.WriteBits(extraValue, extraDistBits);
        }
    }
}

