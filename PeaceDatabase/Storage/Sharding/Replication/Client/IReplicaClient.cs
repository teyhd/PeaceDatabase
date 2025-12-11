using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Sharding.Client;

namespace PeaceDatabase.Storage.Sharding.Replication.Client;

/// <summary>
/// Расширенный интерфейс клиента с поддержкой репликации.
/// Наследует IShardClient и добавляет методы для репликации и управления.
/// </summary>
public interface IReplicaClient : IShardClient
{
    /// <summary>
    /// Информация о реплике.
    /// </summary>
    ReplicaInfo ReplicaInfo { get; }

    /// <summary>
    /// Получает текущее состояние репликации узла.
    /// </summary>
    Task<ReplicationState> GetReplicationStateAsync(CancellationToken ct = default);

    /// <summary>
    /// Реплицирует операцию записи на узел.
    /// </summary>
    Task<ReplicateResult> ReplicateAsync(ReplicationEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Реплицирует пакет операций записи на узел.
    /// </summary>
    Task<ReplicateBatchResult> ReplicateBatchAsync(IEnumerable<ReplicationEntry> entries, CancellationToken ct = default);

    /// <summary>
    /// Уведомляет узел о его продвижении в primary.
    /// </summary>
    Task PromoteAsync(CancellationToken ct = default);

    /// <summary>
    /// Уведомляет узел о новом primary в наборе реплик.
    /// </summary>
    Task SetPrimaryAsync(string newPrimaryUrl, CancellationToken ct = default);

    /// <summary>
    /// Получает записи WAL для синхронизации отстающей реплики.
    /// </summary>
    Task<IReadOnlyList<ReplicationEntry>> GetWalEntriesAsync(
        string db,
        long fromSeq,
        int limit = 1000,
        CancellationToken ct = default);
}

/// <summary>
/// Текущее состояние репликации узла.
/// </summary>
public sealed class ReplicationState
{
    /// <summary>
    /// Здоров ли узел.
    /// </summary>
    public bool IsHealthy { get; init; }

    /// <summary>
    /// Является ли узел primary.
    /// </summary>
    public bool IsPrimary { get; init; }

    /// <summary>
    /// Последний sequence number.
    /// </summary>
    public long Seq { get; init; }

    /// <summary>
    /// Позиция в WAL (опционально).
    /// </summary>
    public string? WalPosition { get; init; }

    /// <summary>
    /// Время работы узла.
    /// </summary>
    public TimeSpan Uptime { get; init; }

    /// <summary>
    /// URL текущего primary (если узел — реплика).
    /// </summary>
    public string? CurrentPrimaryUrl { get; init; }

    /// <summary>
    /// Отставание от primary в операциях.
    /// </summary>
    public long ReplicationLag { get; init; }

    /// <summary>
    /// Timestamp последней успешной синхронизации.
    /// </summary>
    public DateTimeOffset? LastSyncAt { get; init; }
}

/// <summary>
/// Запись для репликации.
/// </summary>
public sealed class ReplicationEntry
{
    /// <summary>
    /// Тип операции.
    /// </summary>
    public ReplicationOp Op { get; init; }

    /// <summary>
    /// Имя базы данных.
    /// </summary>
    public string Db { get; init; } = string.Empty;

    /// <summary>
    /// ID документа.
    /// </summary>
    public string Id { get; init; } = string.Empty;

    /// <summary>
    /// Ревизия документа.
    /// </summary>
    public string? Rev { get; init; }

    /// <summary>
    /// Sequence number операции.
    /// </summary>
    public long Seq { get; init; }

    /// <summary>
    /// Документ (для Put/Post операций).
    /// </summary>
    public Document? Doc { get; init; }

    /// <summary>
    /// Timestamp операции.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Тип операции репликации.
/// </summary>
public enum ReplicationOp
{
    Put,
    Post,
    Delete,
    CreateDb,
    DeleteDb
}

/// <summary>
/// Результат репликации одной операции.
/// </summary>
public sealed class ReplicateResult
{
    /// <summary>
    /// Успешна ли репликация.
    /// </summary>
    public bool Ok { get; init; }

    /// <summary>
    /// Сообщение об ошибке (если не Ok).
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Seq операции на реплике.
    /// </summary>
    public long Seq { get; init; }

    public static ReplicateResult Success(long seq) => new() { Ok = true, Seq = seq };
    public static ReplicateResult Failure(string error) => new() { Ok = false, Error = error };
}

/// <summary>
/// Результат репликации пакета операций.
/// </summary>
public sealed class ReplicateBatchResult
{
    /// <summary>
    /// Успешна ли репликация всего пакета.
    /// </summary>
    public bool Ok { get; init; }

    /// <summary>
    /// Количество успешно реплицированных операций.
    /// </summary>
    public int SuccessCount { get; init; }

    /// <summary>
    /// Количество неудачных операций.
    /// </summary>
    public int FailedCount { get; init; }

    /// <summary>
    /// Ошибки по операциям (seq -> error).
    /// </summary>
    public Dictionary<long, string> Errors { get; init; } = new();

    /// <summary>
    /// Последний успешно реплицированный seq.
    /// </summary>
    public long LastSeq { get; init; }
}

