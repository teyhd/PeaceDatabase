namespace PeaceDatabase.Storage.Sharding.Replication.Configuration;

/// <summary>
/// Конфигурация репликации для обеспечения отказоустойчивости.
/// </summary>
public sealed class ReplicationOptions
{
    /// <summary>
    /// Включена ли репликация.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Количество реплик для каждого шарда (не включая primary).
    /// Например, 2 означает 1 primary + 2 реплики = 3 копии данных.
    /// </summary>
    public int ReplicaCount { get; set; } = 2;

    /// <summary>
    /// Количество подтверждений, необходимых для успешной записи (кворум).
    /// Рекомендуется (ReplicaCount + 1) / 2 + 1 для большинства.
    /// </summary>
    public int WriteQuorum { get; set; } = 2;

    /// <summary>
    /// Количество узлов для чтения (1 = читать только с одного узла).
    /// </summary>
    public int ReadQuorum { get; set; } = 1;

    /// <summary>
    /// Таймаут ожидания при failover (в миллисекундах).
    /// </summary>
    public int FailoverTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// Интервал проверки здоровья реплик (в миллисекундах).
    /// </summary>
    public int HealthCheckIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Максимально допустимое отставание репликации (в миллисекундах).
    /// Реплики с большим отставанием помечаются как Lagging.
    /// </summary>
    public int MaxReplicationLagMs { get; set; } = 10000;

    /// <summary>
    /// Количество пропущенных health check'ов до признания узла недоступным.
    /// </summary>
    public int UnhealthyThreshold { get; set; } = 3;

    /// <summary>
    /// Режим синхронизации записей.
    /// </summary>
    public SyncMode SyncMode { get; set; } = SyncMode.Quorum;

    /// <summary>
    /// Включить ли балансировку чтений между репликами.
    /// </summary>
    public bool ReadLoadBalancing { get; set; } = true;

    /// <summary>
    /// Индекс реплики в наборе (0 = primary candidate).
    /// Null означает, что инстанс является роутером.
    /// </summary>
    public int? CurrentReplicaIndex { get; set; }

    // ==================== Raft Consensus Options ====================

    /// <summary>
    /// Включён ли Raft консенсус (heartbeats, elections).
    /// </summary>
    public bool RaftEnabled { get; set; } = true;

    /// <summary>
    /// Интервал отправки heartbeat лидером (в миллисекундах).
    /// Рекомендуется: HeartbeatIntervalMs &lt;&lt; ElectionTimeoutMinMs
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 150;

    /// <summary>
    /// Минимальный таймаут выборов (в миллисекундах).
    /// Если follower не получает heartbeat за это время, начинает выборы.
    /// </summary>
    public int ElectionTimeoutMinMs { get; set; } = 300;

    /// <summary>
    /// Максимальный таймаут выборов (в миллисекундах).
    /// Фактический таймаут выбирается случайно между Min и Max.
    /// Рандомизация предотвращает split vote.
    /// </summary>
    public int ElectionTimeoutMaxMs { get; set; } = 500;

    /// <summary>
    /// URL-адреса других узлов в Raft кластере (peers).
    /// Используется data nodes для автоматических выборов.
    /// Формат: "http://host1:port,http://host2:port"
    /// </summary>
    public List<string> RaftPeers { get; set; } = new();

    /// <summary>
    /// URL текущего узла (для идентификации себя среди peers).
    /// </summary>
    public string? RaftSelfUrl { get; set; }
}

/// <summary>
/// Режим синхронизации записей.
/// </summary>
public enum SyncMode
{
    /// <summary>
    /// Синхронная репликация: запись ждёт подтверждения от всех реплик.
    /// Максимальная консистентность, высокая задержка.
    /// </summary>
    Sync,

    /// <summary>
    /// Асинхронная репликация: запись возвращается после записи на primary.
    /// Низкая задержка, возможна потеря данных при failover.
    /// </summary>
    Async,

    /// <summary>
    /// Кворумная репликация: запись ждёт подтверждения от большинства реплик.
    /// Баланс между консистентностью и задержкой.
    /// </summary>
    Quorum
}

