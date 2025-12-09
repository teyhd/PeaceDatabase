using System.Runtime.CompilerServices;
using System.Text;

namespace PeaceDatabase.Storage.Sharding.Routing;

/// <summary>
/// MurmurHash3 x86 32-bit implementation.
/// Быстрая некриптографическая хеш-функция, оптимальная для шардирования.
/// </summary>
public static class MurmurHash3
{
    private const uint C1 = 0xcc9e2d51;
    private const uint C2 = 0x1b873593;
    private const uint Seed = 0x9747b28c; // фиксированный seed для консистентности

    /// <summary>
    /// Вычисляет MurmurHash3 для строки (UTF-8).
    /// </summary>
    public static uint Hash(string key)
    {
        if (string.IsNullOrEmpty(key))
            return Seed;

        var bytes = Encoding.UTF8.GetBytes(key);
        return Hash(bytes);
    }

    /// <summary>
    /// Вычисляет MurmurHash3 для массива байтов.
    /// </summary>
    public static uint Hash(byte[] data)
    {
        return Hash(data.AsSpan());
    }

    /// <summary>
    /// Вычисляет MurmurHash3 для Span байтов.
    /// </summary>
    public static uint Hash(ReadOnlySpan<byte> data)
    {
        int length = data.Length;
        int nblocks = length / 4;
        uint h1 = Seed;

        // body - обрабатываем блоки по 4 байта
        for (int i = 0; i < nblocks; i++)
        {
            uint k1 = GetBlock(data, i * 4);

            k1 *= C1;
            k1 = RotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = RotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail - обрабатываем оставшиеся байты
        int tailIndex = nblocks * 4;
        uint k1Tail = 0;

        switch (length & 3)
        {
            case 3:
                k1Tail ^= (uint)data[tailIndex + 2] << 16;
                goto case 2;
            case 2:
                k1Tail ^= (uint)data[tailIndex + 1] << 8;
                goto case 1;
            case 1:
                k1Tail ^= data[tailIndex];
                k1Tail *= C1;
                k1Tail = RotateLeft(k1Tail, 15);
                k1Tail *= C2;
                h1 ^= k1Tail;
                break;
        }

        // finalization
        h1 ^= (uint)length;
        h1 = FMix32(h1);

        return h1;
    }

    /// <summary>
    /// Вычисляет хеш и возвращает неотрицательное значение.
    /// </summary>
    public static int HashPositive(string key)
    {
        return (int)(Hash(key) & 0x7FFFFFFF);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint GetBlock(ReadOnlySpan<byte> data, int index)
    {
        return (uint)data[index]
             | ((uint)data[index + 1] << 8)
             | ((uint)data[index + 2] << 16)
             | ((uint)data[index + 3] << 24);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint RotateLeft(uint x, int r)
    {
        return (x << r) | (x >> (32 - r));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint FMix32(uint h)
    {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
}

