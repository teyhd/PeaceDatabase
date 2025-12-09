using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Discovery;

/// <summary>
/// Интерфейс Service Discovery для обнаружения шардов в кластере.
/// </summary>
public interface IShardDiscovery : IDisposable
{
    /// <summary>
    /// Получить текущий список шардов.
    /// </summary>
    IReadOnlyList<ShardInfo> GetShards();

    /// <summary>
    /// Обновить список шардов (для динамического discovery).
    /// </summary>
    Task RefreshAsync(CancellationToken ct = default);

    /// <summary>
    /// Событие при изменении списка шардов.
    /// </summary>
    event EventHandler<ShardListChangedEventArgs>? ShardsChanged;
}

/// <summary>
/// Аргументы события изменения списка шардов.
/// </summary>
public sealed class ShardListChangedEventArgs : EventArgs
{
    public IReadOnlyList<ShardInfo> Shards { get; }
    public IReadOnlyList<ShardInfo> AddedShards { get; }
    public IReadOnlyList<ShardInfo> RemovedShards { get; }

    public ShardListChangedEventArgs(
        IReadOnlyList<ShardInfo> shards,
        IReadOnlyList<ShardInfo> addedShards,
        IReadOnlyList<ShardInfo> removedShards)
    {
        Shards = shards;
        AddedShards = addedShards;
        RemovedShards = removedShards;
    }
}

