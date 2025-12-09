namespace PeaceDatabase.Storage.Sharding.Configuration;

/// <summary>
/// Информация о шарде в кластере.
/// </summary>
public sealed class ShardInfo
{
    /// <summary>
    /// Уникальный идентификатор шарда (0, 1, 2, ...).
    /// </summary>
    public int Id { get; init; }

    /// <summary>
    /// Базовый URL для HTTP-запросов к шарду.
    /// </summary>
    public string BaseUrl { get; init; } = string.Empty;

    /// <summary>
    /// Текущий статус шарда.
    /// </summary>
    public ShardStatus Status { get; set; } = ShardStatus.Unknown;

    /// <summary>
    /// Является ли шард локальным (в том же процессе).
    /// </summary>
    public bool IsLocal { get; init; }

    /// <summary>
    /// Время последней успешной проверки доступности.
    /// </summary>
    public DateTimeOffset? LastHealthCheck { get; set; }

    /// <summary>
    /// Количество документов на шарде (если известно).
    /// </summary>
    public long? DocumentCount { get; set; }

    public override string ToString() =>
        $"Shard[{Id}] {(IsLocal ? "local" : BaseUrl)} ({Status})";
}

/// <summary>
/// Статус шарда.
/// </summary>
public enum ShardStatus
{
    /// <summary>
    /// Статус неизвестен.
    /// </summary>
    Unknown,

    /// <summary>
    /// Шард доступен и работает нормально.
    /// </summary>
    Healthy,

    /// <summary>
    /// Шард недоступен.
    /// </summary>
    Unhealthy,

    /// <summary>
    /// Шард в процессе инициализации.
    /// </summary>
    Initializing,

    /// <summary>
    /// Шард выведен из эксплуатации.
    /// </summary>
    Draining
}

