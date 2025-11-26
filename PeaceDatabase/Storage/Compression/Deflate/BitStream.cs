// File: Storage/Compression/Deflate/BitStream.cs
namespace PeaceDatabase.Storage.Compression.Deflate;

/// <summary>
/// Bit writer for DEFLATE: writes bits LSB-first (least significant bit first).
/// </summary>
public sealed class BitWriter
{
    private readonly MemoryStream _stream;
    private int _bitBuffer;
    private int _bitCount;

    public BitWriter(MemoryStream stream)
    {
        _stream = stream;
        _bitBuffer = 0;
        _bitCount = 0;
    }

    /// <summary>
    /// Write bits LSB-first.
    /// </summary>
    public void WriteBits(int value, int numBits)
    {
        _bitBuffer |= (value & ((1 << numBits) - 1)) << _bitCount;
        _bitCount += numBits;

        while (_bitCount >= 8)
        {
            _stream.WriteByte((byte)(_bitBuffer & 0xFF));
            _bitBuffer >>= 8;
            _bitCount -= 8;
        }
    }

    /// <summary>
    /// Write bits MSB-first (for Huffman codes).
    /// </summary>
    public void WriteBitsReversed(int code, int numBits)
    {
        int reversed = 0;
        for (int i = 0; i < numBits; i++)
        {
            reversed = (reversed << 1) | (code & 1);
            code >>= 1;
        }
        WriteBits(reversed, numBits);
    }

    /// <summary>
    /// Flush remaining bits, padding with zeros to byte boundary.
    /// </summary>
    public void Flush()
    {
        if (_bitCount > 0)
        {
            _stream.WriteByte((byte)(_bitBuffer & 0xFF));
            _bitBuffer = 0;
            _bitCount = 0;
        }
    }

    /// <summary>
    /// Align to byte boundary without writing (for stored blocks).
    /// </summary>
    public void AlignToByte()
    {
        if (_bitCount > 0)
        {
            _stream.WriteByte((byte)(_bitBuffer & 0xFF));
            _bitBuffer = 0;
            _bitCount = 0;
        }
    }
}

/// <summary>
/// Bit reader for DEFLATE: reads bits LSB-first.
/// Supports "peeking" bits and consuming only what's needed.
/// Uses unsigned arithmetic to avoid sign-related bit shifting issues.
/// </summary>
public sealed class BitReader
{
    private readonly byte[] _data;
    private int _bytePos;
    private uint _bitBuffer;  // Use uint to avoid sign issues
    private int _bitCount;

    public BitReader(byte[] data)
    {
        _data = data;
        _bytePos = 0;
        _bitBuffer = 0;
        _bitCount = 0;
    }

    public int BytePosition => _bytePos;
    public bool HasMoreData => _bytePos < _data.Length || _bitCount > 0;

    /// <summary>
    /// Ensure at least numBits are in the buffer.
    /// </summary>
    private void EnsureBits(int numBits)
    {
        while (_bitCount < numBits)
        {
            if (_bytePos >= _data.Length)
                throw new InvalidDataException("Unexpected end of data");
            _bitBuffer |= (uint)_data[_bytePos++] << _bitCount;
            _bitCount += 8;
        }
    }

    /// <summary>
    /// Peek bits without consuming them.
    /// </summary>
    public int PeekBits(int numBits)
    {
        EnsureBits(numBits);
        return (int)(_bitBuffer & ((1u << numBits) - 1));
    }

    /// <summary>
    /// Consume bits (must have been peeked first or ensure buffer has them).
    /// </summary>
    public void ConsumeBits(int numBits)
    {
        _bitBuffer >>= numBits;
        _bitCount -= numBits;
    }

    /// <summary>
    /// Read bits LSB-first.
    /// </summary>
    public int ReadBits(int numBits)
    {
        EnsureBits(numBits);
        int result = (int)(_bitBuffer & ((1u << numBits) - 1));
        _bitBuffer >>= numBits;
        _bitCount -= numBits;
        return result;
    }

    /// <summary>
    /// Read Huffman code bits (need to reverse for lookup).
    /// </summary>
    public int ReadBitsReversed(int numBits)
    {
        int value = ReadBits(numBits);
        int reversed = 0;
        for (int i = 0; i < numBits; i++)
        {
            reversed = (reversed << 1) | (value & 1);
            value >>= 1;
        }
        return reversed;
    }

    /// <summary>
    /// Align to next byte boundary (discard remaining bits in current byte).
    /// </summary>
    public void AlignToByte()
    {
        _bitBuffer = 0;
        _bitCount = 0;
    }

    /// <summary>
    /// Read a raw byte (after aligning).
    /// </summary>
    public byte ReadByte()
    {
        AlignToByte();
        if (_bytePos >= _data.Length)
            throw new InvalidDataException("Unexpected end of data");
        return _data[_bytePos++];
    }

    /// <summary>
    /// Read a 16-bit little-endian value (after aligning).
    /// </summary>
    public ushort ReadUInt16LE()
    {
        AlignToByte();
        if (_bytePos + 2 > _data.Length)
            throw new InvalidDataException("Unexpected end of data");
        ushort value = (ushort)(_data[_bytePos] | (_data[_bytePos + 1] << 8));
        _bytePos += 2;
        return value;
    }
}
