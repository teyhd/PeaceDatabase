namespace PeaceDatabase.Storage.Sharding.Replication.Configuration;

/// <summary>
/// Конфигурация набора реплик для шарда (для статической конфигурации).
/// </summary>
public sealed class ReplicaSetConfig
{
    /// <summary>
    /// Идентификатор шарда.
    /// </summary>
    public int ShardId { get; set; }

    /// <summary>
    /// URL primary узла.
    /// </summary>
    public string Primary { get; set; } = string.Empty;

    /// <summary>
    /// URLs реплик.
    /// </summary>
    public List<string> Replicas { get; set; } = new();
}

/// <summary>
/// Конечная точка реплики.
/// </summary>
public sealed class ReplicaEndpoint
{
    /// <summary>
    /// Идентификатор шарда, к которому принадлежит реплика.
    /// </summary>
    public int ShardId { get; set; }

    /// <summary>
    /// Индекс реплики в наборе (0 = primary, 1+ = реплики).
    /// </summary>
    public int ReplicaIndex { get; set; }

    /// <summary>
    /// Базовый URL реплики.
    /// </summary>
    public string BaseUrl { get; set; } = string.Empty;

    /// <summary>
    /// Является ли эта реплика primary (при инициализации).
    /// </summary>
    public bool IsPrimary => ReplicaIndex == 0;

    public override string ToString() =>
        $"Shard[{ShardId}].Replica[{ReplicaIndex}] @ {BaseUrl}";
}

