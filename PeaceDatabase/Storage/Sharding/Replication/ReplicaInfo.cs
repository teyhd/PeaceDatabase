using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Информация о реплике в наборе реплик шарда.
/// Расширяет ShardInfo данными о репликации.
/// </summary>
public sealed class ReplicaInfo
{
    /// <summary>
    /// Идентификатор шарда, к которому принадлежит реплика.
    /// </summary>
    public int ShardId { get; init; }

    /// <summary>
    /// Индекс реплики в наборе (0 = первоначальный primary candidate).
    /// </summary>
    public int ReplicaIndex { get; init; }

    /// <summary>
    /// Базовый URL для HTTP-запросов к реплике.
    /// </summary>
    public string BaseUrl { get; init; } = string.Empty;

    /// <summary>
    /// Является ли эта реплика текущим primary.
    /// </summary>
    public bool IsPrimary { get; set; }

    /// <summary>
    /// Текущее состояние синхронизации реплики.
    /// </summary>
    public ReplicaSyncState SyncState { get; set; } = ReplicaSyncState.Unknown;

    /// <summary>
    /// Статус доступности реплики.
    /// </summary>
    public ReplicaHealthStatus HealthStatus { get; set; } = ReplicaHealthStatus.Unknown;

    /// <summary>
    /// Последний известный sequence number для расчёта отставания.
    /// </summary>
    public long LastSeq { get; set; }

    /// <summary>
    /// Время последнего успешного health check.
    /// </summary>
    public DateTimeOffset? LastHealthCheck { get; set; }

    /// <summary>
    /// Количество последовательных неудачных health check'ов.
    /// </summary>
    public int FailedHealthChecks { get; set; }

    /// <summary>
    /// Время, когда реплика стала primary (если IsPrimary = true).
    /// </summary>
    public DateTimeOffset? PromotedAt { get; set; }

    /// <summary>
    /// Отставание репликации в миллисекундах (приблизительно).
    /// </summary>
    public long ReplicationLagMs { get; set; }

    /// <summary>
    /// Является ли реплика локальной (в том же процессе).
    /// </summary>
    public bool IsLocal { get; init; }

    /// <summary>
    /// Уникальный идентификатор реплики в формате "shardId-replicaIndex".
    /// </summary>
    public string UniqueId => $"{ShardId}-{ReplicaIndex}";

    /// <summary>
    /// Доступна ли реплика для операций.
    /// </summary>
    public bool IsAvailable => HealthStatus == ReplicaHealthStatus.Healthy &&
                               SyncState != ReplicaSyncState.Offline;

    /// <summary>
    /// Может ли реплика быть избрана primary.
    /// </summary>
    public bool CanBeElected => IsAvailable && SyncState == ReplicaSyncState.InSync;

    public override string ToString() =>
        $"Replica[{ShardId}.{ReplicaIndex}] {(IsPrimary ? "PRIMARY" : "REPLICA")} " +
        $"@ {(IsLocal ? "local" : BaseUrl)} ({HealthStatus}, {SyncState})";
}

/// <summary>
/// Состояние синхронизации реплики.
/// </summary>
public enum ReplicaSyncState
{
    /// <summary>
    /// Состояние неизвестно.
    /// </summary>
    Unknown,

    /// <summary>
    /// Реплика полностью синхронизирована с primary.
    /// </summary>
    InSync,

    /// <summary>
    /// Реплика отстаёт от primary, но догоняет.
    /// </summary>
    Lagging,

    /// <summary>
    /// Реплика в процессе начальной синхронизации.
    /// </summary>
    Syncing,

    /// <summary>
    /// Реплика недоступна.
    /// </summary>
    Offline
}

/// <summary>
/// Статус здоровья реплики.
/// </summary>
public enum ReplicaHealthStatus
{
    /// <summary>
    /// Статус неизвестен.
    /// </summary>
    Unknown,

    /// <summary>
    /// Реплика здорова и отвечает на запросы.
    /// </summary>
    Healthy,

    /// <summary>
    /// Реплика недоступна.
    /// </summary>
    Unhealthy,

    /// <summary>
    /// Реплика в процессе инициализации.
    /// </summary>
    Initializing
}

