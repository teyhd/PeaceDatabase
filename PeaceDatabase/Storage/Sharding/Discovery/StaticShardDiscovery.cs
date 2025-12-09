using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Discovery;

/// <summary>
/// Статический Service Discovery на основе конфигурации из appsettings.
/// Список шардов фиксирован и не меняется динамически.
/// </summary>
public sealed class StaticShardDiscovery : IShardDiscovery
{
    private readonly List<ShardInfo> _shards;
    private bool _disposed;

    public event EventHandler<ShardListChangedEventArgs>? ShardsChanged;

    public StaticShardDiscovery(ShardingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _shards = options.Shards
            .Select(e => new ShardInfo
            {
                Id = e.Id,
                BaseUrl = e.BaseUrl,
                IsLocal = false,
                Status = ShardStatus.Unknown
            })
            .OrderBy(s => s.Id)
            .ToList();

        // Если шарды не заданы, генерируем на основе count
        if (_shards.Count == 0 && options.ShardCount > 0)
        {
            for (int i = 0; i < options.ShardCount; i++)
            {
                _shards.Add(new ShardInfo
                {
                    Id = i,
                    BaseUrl = $"http://{options.Discovery.ServicePrefix}{i}:{options.Discovery.Port}",
                    IsLocal = false,
                    Status = ShardStatus.Unknown
                });
            }
        }
    }

    public IReadOnlyList<ShardInfo> GetShards() => _shards;

    public Task RefreshAsync(CancellationToken ct = default)
    {
        // Статический discovery не обновляется
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _shards.Clear();
    }
}

