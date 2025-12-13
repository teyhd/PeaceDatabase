using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Координатор репликации. Управляет наборами реплик, failover'ом и маршрутизацией.
/// </summary>
public sealed class ReplicationCoordinator : IDisposable
{
    private readonly ShardingOptions _shardingOptions;
    private readonly ILogger<ReplicationCoordinator>? _logger;
    private readonly Func<ReplicaInfo, IReplicaClient> _clientFactory;
    private readonly Dictionary<int, ReplicaSet> _replicaSets = new();
    private readonly Dictionary<string, IReplicaClient> _clients = new();
    private readonly LeaderElection _leaderElection;
    private readonly object _lock = new();
    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Событие при смене primary в любом наборе реплик.
    /// </summary>
    public event EventHandler<PrimaryChangedEventArgs>? PrimaryChanged;

    /// <summary>
    /// Событие при завершении failover.
    /// </summary>
    public event EventHandler<FailoverCompletedEventArgs>? FailoverCompleted;

    public ReplicationCoordinator(
        ShardingOptions shardingOptions,
        Func<ReplicaInfo, IReplicaClient> clientFactory,
        ILogger<ReplicationCoordinator>? logger = null)
    {
        _shardingOptions = shardingOptions ?? throw new ArgumentNullException(nameof(shardingOptions));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger;

        _leaderElection = new LeaderElection(
            shardingOptions.Replication,
            clientFactory,
            logger != null ? new LoggerWrapper<LeaderElection>(logger) : null);
    }

    /// <summary>
    /// Инициализирует координатор на основе конфигурации.
    /// </summary>
    public void Initialize()
    {
        lock (_lock)
        {
            if (_initialized)
                return;

            if (_shardingOptions.Mode == ShardingMode.Local)
            {
                InitializeLocalMode();
            }
            else
            {
                InitializeDistributedMode();
            }

            _initialized = true;
            _logger?.LogInformation(
                "ReplicationCoordinator initialized with {ShardCount} shards, {ReplicaCount} replicas per shard",
                _replicaSets.Count, _shardingOptions.Replication.ReplicaCount);
        }
    }

    private void InitializeLocalMode()
    {
        for (int i = 0; i < _shardingOptions.ShardCount; i++)
        {
            var replicaSet = new ReplicaSet(i, _shardingOptions.Replication, _logger);
            replicaSet.InitializeLocal(_shardingOptions.Replication.ReplicaCount);
            replicaSet.PrimaryChanged += OnPrimaryChanged;
            _replicaSets[i] = replicaSet;
        }
    }

    private void InitializeDistributedMode()
    {
        // Инициализация из статической конфигурации ReplicaSets
        foreach (var config in _shardingOptions.ReplicaSets)
        {
            var replicaSet = new ReplicaSet(config.ShardId, _shardingOptions.Replication, _logger);
            replicaSet.Initialize(config);
            replicaSet.PrimaryChanged += OnPrimaryChanged;
            _replicaSets[config.ShardId] = replicaSet;
        }

        // Если ReplicaSets не заданы, создаём из Shards (группируем по ShardId)
        if (_replicaSets.Count == 0 && _shardingOptions.Shards.Count > 0)
        {
            // Группируем узлы по ShardId
            var groupedByShardId = _shardingOptions.Shards
                .GroupBy(s => s.Id)
                .ToDictionary(g => g.Key, g => g.Select(s => s.BaseUrl).ToList());

            foreach (var (shardId, urls) in groupedByShardId)
            {
                // Первый URL - primary, остальные - replicas
                var primary = urls.FirstOrDefault() ?? string.Empty;
                var replicas = urls.Skip(1).ToList();

                var replicaSet = new ReplicaSet(shardId, _shardingOptions.Replication, _logger);
                replicaSet.Initialize(new ReplicaSetConfig
                {
                    ShardId = shardId,
                    Primary = primary,
                    Replicas = replicas
                });
                replicaSet.PrimaryChanged += OnPrimaryChanged;
                _replicaSets[shardId] = replicaSet;

                _logger?.LogInformation(
                    "Initialized ReplicaSet for shard {ShardId}: primary={Primary}, replicas={ReplicaCount}",
                    shardId, primary, replicas.Count);
            }
        }
    }

    private void OnPrimaryChanged(object? sender, PrimaryChangedEventArgs e)
    {
        PrimaryChanged?.Invoke(this, e);
    }

    /// <summary>
    /// Получает набор реплик для шарда.
    /// </summary>
    public ReplicaSet? GetReplicaSet(int shardId)
    {
        lock (_lock)
        {
            return _replicaSets.GetValueOrDefault(shardId);
        }
    }

    /// <summary>
    /// Получает все наборы реплик.
    /// </summary>
    public IReadOnlyList<ReplicaSet> GetAllReplicaSets()
    {
        lock (_lock)
        {
            return _replicaSets.Values.ToList();
        }
    }

    /// <summary>
    /// Получает клиент для primary узла указанного шарда.
    /// </summary>
    public IReplicaClient? GetPrimaryClient(int shardId)
    {
        var replicaSet = GetReplicaSet(shardId);
        var primary = replicaSet?.Primary;
        if (primary == null)
            return null;

        return GetOrCreateClient(primary);
    }

    /// <summary>
    /// Получает клиенты для всех узлов шарда (для кворумных операций).
    /// </summary>
    public IReadOnlyList<IReplicaClient> GetWriteClients(int shardId)
    {
        var replicaSet = GetReplicaSet(shardId);
        if (replicaSet == null)
            return Array.Empty<IReplicaClient>();

        var targets = replicaSet.GetWriteTargets();
        return targets.Select(GetOrCreateClient).ToList();
    }

    /// <summary>
    /// Получает клиенты для чтения (с балансировкой нагрузки).
    /// </summary>
    public IReadOnlyList<IReplicaClient> GetReadClients(int shardId)
    {
        var replicaSet = GetReplicaSet(shardId);
        if (replicaSet == null)
            return Array.Empty<IReplicaClient>();

        var targets = replicaSet.GetReadTargets();
        return targets.Select(GetOrCreateClient).ToList();
    }

    /// <summary>
    /// Выполняет failover для шарда: выбирает нового primary и обновляет маршрутизацию.
    /// </summary>
    public async Task<bool> FailoverAsync(int shardId, CancellationToken ct = default)
    {
        var replicaSet = GetReplicaSet(shardId);
        if (replicaSet == null)
        {
            _logger?.LogError("Cannot failover: shard {ShardId} not found", shardId);
            return false;
        }

        var oldPrimary = replicaSet.Primary;

        _logger?.LogWarning(
            "Starting failover for shard {ShardId}, current primary: {Primary}",
            shardId, oldPrimary?.UniqueId ?? "none");

        try
        {
            // Помечаем текущий primary как недоступный
            replicaSet.MarkPrimaryDown();

            // Проводим выборы нового primary
            var newPrimary = await _leaderElection.ElectPrimaryAsync(replicaSet, ct);

            if (newPrimary == null)
            {
                _logger?.LogError("Failover failed for shard {ShardId}: no eligible replica found", shardId);
                FailoverCompleted?.Invoke(this, new FailoverCompletedEventArgs(shardId, false, oldPrimary, null, "No eligible replica"));
                return false;
            }

            // Уведомляем все реплики о новом primary
            await NotifyReplicasAboutNewPrimaryAsync(replicaSet, newPrimary, ct);

            _logger?.LogInformation(
                "Failover completed for shard {ShardId}: {OldPrimary} -> {NewPrimary}",
                shardId, oldPrimary?.UniqueId ?? "none", newPrimary.UniqueId);

            FailoverCompleted?.Invoke(this, new FailoverCompletedEventArgs(shardId, true, oldPrimary, newPrimary, null));
            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failover failed for shard {ShardId}", shardId);
            FailoverCompleted?.Invoke(this, new FailoverCompletedEventArgs(shardId, false, oldPrimary, null, ex.Message));
            return false;
        }
    }

    /// <summary>
    /// Обрабатывает событие падения primary от HealthMonitor.
    /// </summary>
    public async Task HandlePrimaryDownAsync(int shardId, CancellationToken ct = default)
    {
        _logger?.LogWarning("Handling primary down event for shard {ShardId}", shardId);
        await FailoverAsync(shardId, ct);
    }

    private async Task NotifyReplicasAboutNewPrimaryAsync(ReplicaSet replicaSet, ReplicaInfo newPrimary, CancellationToken ct)
    {
        var tasks = replicaSet.AllReplicas
            .Where(r => r.UniqueId != newPrimary.UniqueId && r.IsAvailable)
            .Select(async replica =>
            {
                try
                {
                    var client = GetOrCreateClient(replica);
                    await client.SetPrimaryAsync(newPrimary.BaseUrl, ct);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(
                        "Failed to notify replica {ReplicaId} about new primary: {Error}",
                        replica.UniqueId, ex.Message);
                }
            });

        await Task.WhenAll(tasks);
    }

    private IReplicaClient GetOrCreateClient(ReplicaInfo replica)
    {
        lock (_lock)
        {
            if (_clients.TryGetValue(replica.UniqueId, out var existing))
                return existing;

            var client = _clientFactory(replica);
            _clients[replica.UniqueId] = client;
            return client;
        }
    }

    /// <summary>
    /// Проверяет здоровье всех primary и инициирует failover при необходимости.
    /// </summary>
    public async Task CheckAndFailoverAsync(CancellationToken ct = default)
    {
        foreach (var replicaSet in GetAllReplicaSets())
        {
            var primary = replicaSet.Primary;
            if (primary == null || primary.HealthStatus != ReplicaHealthStatus.Healthy)
            {
                await FailoverAsync(replicaSet.ShardId, ct);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_lock)
        {
            foreach (var replicaSet in _replicaSets.Values)
            {
                replicaSet.PrimaryChanged -= OnPrimaryChanged;
            }
            _replicaSets.Clear();

            foreach (var client in _clients.Values)
            {
                client.Dispose();
            }
            _clients.Clear();
        }
    }
}

/// <summary>
/// Аргументы события завершения failover.
/// </summary>
public sealed class FailoverCompletedEventArgs : EventArgs
{
    public int ShardId { get; }
    public bool Success { get; }
    public ReplicaInfo? OldPrimary { get; }
    public ReplicaInfo? NewPrimary { get; }
    public string? Error { get; }

    public FailoverCompletedEventArgs(
        int shardId,
        bool success,
        ReplicaInfo? oldPrimary,
        ReplicaInfo? newPrimary,
        string? error)
    {
        ShardId = shardId;
        Success = success;
        OldPrimary = oldPrimary;
        NewPrimary = newPrimary;
        Error = error;
    }
}

/// <summary>
/// Обёртка для ILogger для передачи в LeaderElection.
/// </summary>
internal sealed class LoggerWrapper<T> : ILogger<T>
{
    private readonly ILogger _inner;

    public LoggerWrapper(ILogger inner) => _inner = inner;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => _inner.BeginScope(state);
    public bool IsEnabled(LogLevel logLevel) => _inner.IsEnabled(logLevel);
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => _inner.Log(logLevel, eventId, state, exception, formatter);
}

