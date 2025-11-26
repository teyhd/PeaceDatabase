// File: Storage/Compression/Deflate/Lz77Encoder.cs
namespace PeaceDatabase.Storage.Compression.Deflate;

/// <summary>
/// LZ77 token: either a literal byte or a (length, distance) match.
/// </summary>
public readonly struct Lz77Token
{
    public readonly bool IsLiteral;
    public readonly byte Literal;
    public readonly int Length;
    public readonly int Distance;

    private Lz77Token(bool isLiteral, byte literal, int length, int distance)
    {
        IsLiteral = isLiteral;
        Literal = literal;
        Length = length;
        Distance = distance;
    }

    public static Lz77Token CreateLiteral(byte b) => new(true, b, 0, 0);
    public static Lz77Token CreateMatch(int length, int distance) => new(false, 0, length, distance);
}

/// <summary>
/// LZ77 encoder with sliding window for DEFLATE compression.
/// Uses hash chain for fast match finding.
/// </summary>
public sealed class Lz77Encoder
{
    private const int WindowSize = 32768;      // 32KB sliding window
    private const int MaxMatch = 258;          // Maximum match length
    private const int MinMatch = 3;            // Minimum match length
    private const int HashBits = 15;
    private const int HashSize = 1 << HashBits;
    private const int HashMask = HashSize - 1;
    private const int MaxChainLength = 128;    // Limit search depth for speed

    private readonly byte[] _window;
    private readonly int[] _head;              // Hash -> position
    private readonly int[] _prev;              // Chain: position -> previous position with same hash
    private int _windowFill;

    public Lz77Encoder()
    {
        _window = new byte[WindowSize * 2];    // Double buffer for easier wraparound
        _head = new int[HashSize];
        _prev = new int[WindowSize];
        Array.Fill(_head, -1);
        _windowFill = 0;
    }

    /// <summary>
    /// Encode input data into LZ77 tokens.
    /// </summary>
    public List<Lz77Token> Encode(ReadOnlySpan<byte> input)
    {
        var tokens = new List<Lz77Token>(input.Length);
        int pos = 0;

        // Initialize window with input
        int copyLen = Math.Min(input.Length, _window.Length);
        input.Slice(0, copyLen).CopyTo(_window);
        _windowFill = copyLen;

        while (pos < input.Length)
        {
            if (pos + MinMatch <= input.Length)
            {
                var (matchLen, matchDist) = FindBestMatch(pos, input);
                
                if (matchLen >= MinMatch)
                {
                    tokens.Add(Lz77Token.CreateMatch(matchLen, matchDist));
                    
                    // Update hash chain for all positions in match
                    for (int i = 0; i < matchLen; i++)
                    {
                        if (pos + i + MinMatch <= input.Length)
                            UpdateHash(pos + i, input);
                    }
                    
                    pos += matchLen;
                    continue;
                }
            }

            // No match found, emit literal
            tokens.Add(Lz77Token.CreateLiteral(input[pos]));
            UpdateHash(pos, input);
            pos++;
        }

        return tokens;
    }

    private int ComputeHash(ReadOnlySpan<byte> input, int pos)
    {
        if (pos + 2 >= input.Length)
            return 0;
        
        // Simple 3-byte hash
        return ((input[pos] << 10) ^ (input[pos + 1] << 5) ^ input[pos + 2]) & HashMask;
    }

    private void UpdateHash(int pos, ReadOnlySpan<byte> input)
    {
        if (pos + MinMatch > input.Length)
            return;

        int hash = ComputeHash(input, pos);
        int windowIdx = pos & (WindowSize - 1);
        
        _prev[windowIdx] = _head[hash];
        _head[hash] = pos;
    }

    private (int Length, int Distance) FindBestMatch(int pos, ReadOnlySpan<byte> input)
    {
        if (pos + MinMatch > input.Length)
            return (0, 0);

        int hash = ComputeHash(input, pos);
        int matchPos = _head[hash];
        int bestLen = MinMatch - 1;
        int bestDist = 0;
        int chainLen = 0;
        int minPos = Math.Max(0, pos - WindowSize);

        while (matchPos >= minPos && matchPos < pos && chainLen < MaxChainLength)
        {
            int dist = pos - matchPos;
            if (dist > 0 && dist <= WindowSize)
            {
                int len = MatchLength(input, pos, matchPos);
                if (len > bestLen)
                {
                    bestLen = len;
                    bestDist = dist;
                    if (len >= MaxMatch)
                        break;
                }
            }

            int windowIdx = matchPos & (WindowSize - 1);
            int nextMatchPos = _prev[windowIdx];
            
            // Prevent infinite loops and invalid chain references
            // _prev may contain stale values from previous window cycles
            if (nextMatchPos >= matchPos || nextMatchPos < minPos)
                break;
                
            matchPos = nextMatchPos;
            chainLen++;
        }

        if (bestLen >= MinMatch)
            return (bestLen, bestDist);
        
        return (0, 0);
    }

    private int MatchLength(ReadOnlySpan<byte> input, int pos1, int pos2)
    {
        int maxLen = Math.Min(MaxMatch, input.Length - pos1);
        int len = 0;

        while (len < maxLen && input[pos1 + len] == input[pos2 + len])
            len++;

        return len;
    }
}

