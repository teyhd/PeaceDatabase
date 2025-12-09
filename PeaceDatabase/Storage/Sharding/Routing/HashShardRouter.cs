using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Routing;

/// <summary>
/// Hash-based маршрутизатор: shard_id = hash(key) % N.
/// Обеспечивает равномерное распределение ключей по шардам.
/// </summary>
public sealed class HashShardRouter : IShardRouter
{
    private readonly int _shardCount;
    private readonly HashAlgorithmType _algorithm;

    public HashShardRouter(int shardCount, HashAlgorithmType algorithm = HashAlgorithmType.MurmurHash3)
    {
        if (shardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(shardCount), "Shard count must be positive");

        _shardCount = shardCount;
        _algorithm = algorithm;
    }

    public HashShardRouter(ShardingOptions options)
        : this(options.ShardCount, options.HashAlgorithm)
    {
    }

    public int ShardCount => _shardCount;

    public int GetShardId(string key)
    {
        if (string.IsNullOrEmpty(key))
            return 0; // пустой ключ -> шард 0

        int hash = ComputeHash(key);
        return Math.Abs(hash) % _shardCount;
    }

    public Dictionary<int, List<string>> GroupByShards(IEnumerable<string> keys)
    {
        var result = new Dictionary<int, List<string>>();

        foreach (var key in keys)
        {
            int shardId = GetShardId(key);

            if (!result.TryGetValue(shardId, out var list))
            {
                list = new List<string>();
                result[shardId] = list;
            }

            list.Add(key);
        }

        return result;
    }

    public IEnumerable<int> GetAllShardIds()
    {
        for (int i = 0; i < _shardCount; i++)
            yield return i;
    }

    private int ComputeHash(string key)
    {
        return _algorithm switch
        {
            HashAlgorithmType.MurmurHash3 => MurmurHash3.HashPositive(key),
            HashAlgorithmType.Crc32 => ComputeCrc32(key),
            HashAlgorithmType.Sha256Mod => ComputeSha256Mod(key),
            _ => MurmurHash3.HashPositive(key)
        };
    }

    private static int ComputeCrc32(string key)
    {
        // Простая реализация CRC32
        uint crc = 0xFFFFFFFF;
        foreach (byte b in System.Text.Encoding.UTF8.GetBytes(key))
        {
            crc ^= b;
            for (int i = 0; i < 8; i++)
            {
                crc = (crc >> 1) ^ (0xEDB88320 & ~((crc & 1) - 1));
            }
        }
        return (int)(~crc & 0x7FFFFFFF);
    }

    private static int ComputeSha256Mod(string key)
    {
        using var sha = System.Security.Cryptography.SHA256.Create();
        var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(key));
        // берём первые 4 байта как int
        return Math.Abs(BitConverter.ToInt32(hash, 0));
    }
}

