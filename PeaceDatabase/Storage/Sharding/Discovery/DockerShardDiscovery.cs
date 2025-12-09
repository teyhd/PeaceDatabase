using System.Net;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Discovery;

/// <summary>
/// Docker-based Service Discovery.
/// Использует DNS resolution для обнаружения шардов по имени контейнера.
/// </summary>
public sealed class DockerShardDiscovery : IShardDiscovery
{
    private readonly ShardingOptions _options;
    private readonly ILogger<DockerShardDiscovery>? _logger;
    private readonly object _lock = new();
    private List<ShardInfo> _shards = new();
    private bool _disposed;

    public event EventHandler<ShardListChangedEventArgs>? ShardsChanged;

    public DockerShardDiscovery(ShardingOptions options, ILogger<DockerShardDiscovery>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger;

        // Инициализируем известными шардами
        InitializeShards();
    }

    private void InitializeShards()
    {
        lock (_lock)
        {
            _shards = new List<ShardInfo>();
            for (int i = 0; i < _options.ShardCount; i++)
            {
                var hostname = $"{_options.Discovery.ServicePrefix}{i}";
                _shards.Add(new ShardInfo
                {
                    Id = i,
                    BaseUrl = $"http://{hostname}:{_options.Discovery.Port}",
                    IsLocal = false,
                    Status = ShardStatus.Unknown
                });
            }
        }
    }

    public IReadOnlyList<ShardInfo> GetShards()
    {
        lock (_lock)
        {
            return _shards.ToList();
        }
    }

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        var newShards = new List<ShardInfo>();
        var oldShards = GetShards();

        for (int i = 0; i < _options.ShardCount; i++)
        {
            var hostname = $"{_options.Discovery.ServicePrefix}{i}";

            try
            {
                // Пытаемся разрешить DNS имя
                var addresses = await Dns.GetHostAddressesAsync(hostname, ct);
                var isReachable = addresses.Length > 0;

                newShards.Add(new ShardInfo
                {
                    Id = i,
                    BaseUrl = $"http://{hostname}:{_options.Discovery.Port}",
                    IsLocal = false,
                    Status = isReachable ? ShardStatus.Unknown : ShardStatus.Unhealthy
                });

                if (isReachable)
                    _logger?.LogDebug("Shard {ShardId} resolved to {Address}", i, addresses[0]);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to resolve shard {ShardId} hostname: {Hostname}", i, hostname);

                newShards.Add(new ShardInfo
                {
                    Id = i,
                    BaseUrl = $"http://{hostname}:{_options.Discovery.Port}",
                    IsLocal = false,
                    Status = ShardStatus.Unhealthy
                });
            }
        }

        // Определяем изменения
        var addedShards = newShards
            .Where(n => !oldShards.Any(o => o.Id == n.Id))
            .ToList();

        var removedShards = oldShards
            .Where(o => !newShards.Any(n => n.Id == o.Id))
            .ToList();

        // Обновляем список
        lock (_lock)
        {
            _shards = newShards;
        }

        // Уведомляем об изменениях
        if (addedShards.Count > 0 || removedShards.Count > 0)
        {
            ShardsChanged?.Invoke(this, new ShardListChangedEventArgs(newShards, addedShards, removedShards));
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        lock (_lock)
        {
            _shards.Clear();
        }
    }
}

