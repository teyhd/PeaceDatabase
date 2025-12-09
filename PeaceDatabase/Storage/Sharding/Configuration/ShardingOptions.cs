namespace PeaceDatabase.Storage.Sharding.Configuration;

/// <summary>
/// Конфигурация шардирования базы данных.
/// </summary>
public sealed class ShardingOptions
{
    /// <summary>
    /// Включено ли шардирование.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Режим работы: Local (все шарды в одном процессе) или Distributed (шарды в отдельных контейнерах).
    /// </summary>
    public ShardingMode Mode { get; set; } = ShardingMode.Local;

    /// <summary>
    /// Количество шардов в кластере.
    /// </summary>
    public int ShardCount { get; set; } = 3;

    /// <summary>
    /// Идентификатор текущего шарда (для режима Distributed, когда инстанс является шардом).
    /// Null означает, что инстанс является роутером.
    /// </summary>
    public int? CurrentShardId { get; set; }

    /// <summary>
    /// Алгоритм хеширования для распределения ключей.
    /// </summary>
    public HashAlgorithmType HashAlgorithm { get; set; } = HashAlgorithmType.MurmurHash3;

    /// <summary>
    /// Настройки Service Discovery.
    /// </summary>
    public DiscoveryOptions Discovery { get; set; } = new();

    /// <summary>
    /// Статический список шардов (используется при Discovery.Type = Static).
    /// </summary>
    public List<ShardEndpoint> Shards { get; set; } = new();

    /// <summary>
    /// Таймаут HTTP-запросов к шардам (в секундах).
    /// </summary>
    public int RequestTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Количество повторных попыток при ошибке связи с шардом.
    /// </summary>
    public int RetryCount { get; set; } = 3;
}

/// <summary>
/// Режим работы шардирования.
/// </summary>
public enum ShardingMode
{
    /// <summary>
    /// Локальный режим: все шарды в одном процессе (для разработки/тестирования).
    /// </summary>
    Local,

    /// <summary>
    /// Распределённый режим: шарды в отдельных контейнерах/процессах.
    /// </summary>
    Distributed
}

/// <summary>
/// Тип алгоритма хеширования.
/// </summary>
public enum HashAlgorithmType
{
    MurmurHash3,
    Crc32,
    Sha256Mod
}

/// <summary>
/// Настройки Service Discovery.
/// </summary>
public sealed class DiscoveryOptions
{
    /// <summary>
    /// Тип Service Discovery.
    /// </summary>
    public DiscoveryType Type { get; set; } = DiscoveryType.Static;

    /// <summary>
    /// Префикс имени сервиса для Docker DNS (например, "peacedb-shard-").
    /// </summary>
    public string ServicePrefix { get; set; } = "peacedb-shard-";

    /// <summary>
    /// Порт шардов (по умолчанию 8080).
    /// </summary>
    public int Port { get; set; } = 8080;

    /// <summary>
    /// Интервал обновления списка шардов (в секундах).
    /// </summary>
    public int RefreshIntervalSeconds { get; set; } = 30;
}

/// <summary>
/// Тип Service Discovery.
/// </summary>
public enum DiscoveryType
{
    /// <summary>
    /// Статическая конфигурация из appsettings.
    /// </summary>
    Static,

    /// <summary>
    /// Docker DNS resolution по имени контейнера.
    /// </summary>
    Docker
}

/// <summary>
/// Конечная точка шарда (для статической конфигурации).
/// </summary>
public sealed class ShardEndpoint
{
    /// <summary>
    /// Идентификатор шарда (0, 1, 2, ...).
    /// </summary>
    public int Id { get; set; }

    /// <summary>
    /// Базовый URL шарда (например, "http://shard0:8080").
    /// </summary>
    public string BaseUrl { get; set; } = string.Empty;
}

