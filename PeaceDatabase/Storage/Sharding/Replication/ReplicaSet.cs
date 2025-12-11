using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Набор реплик для одного шарда.
/// Управляет primary и репликами, обеспечивает кворумные операции.
/// </summary>
public sealed class ReplicaSet
{
    private readonly object _lock = new();
    private readonly ILogger? _logger;
    private readonly ReplicationOptions _options;
    private ReplicaInfo? _primary;
    private readonly List<ReplicaInfo> _replicas = new();

    /// <summary>
    /// Идентификатор шарда.
    /// </summary>
    public int ShardId { get; }

    /// <summary>
    /// Текущий primary узел (может быть null при failover).
    /// </summary>
    public ReplicaInfo? Primary
    {
        get { lock (_lock) return _primary; }
    }

    /// <summary>
    /// Все реплики (включая primary).
    /// </summary>
    public IReadOnlyList<ReplicaInfo> AllReplicas
    {
        get
        {
            lock (_lock)
            {
                var all = new List<ReplicaInfo>();
                if (_primary != null) all.Add(_primary);
                all.AddRange(_replicas);
                return all;
            }
        }
    }

    /// <summary>
    /// Только реплики (без primary).
    /// </summary>
    public IReadOnlyList<ReplicaInfo> Replicas
    {
        get { lock (_lock) return _replicas.ToList(); }
    }

    /// <summary>
    /// Количество доступных узлов (primary + здоровые реплики).
    /// </summary>
    public int AvailableCount
    {
        get
        {
            lock (_lock)
            {
                int count = _primary?.IsAvailable == true ? 1 : 0;
                count += _replicas.Count(r => r.IsAvailable);
                return count;
            }
        }
    }

    /// <summary>
    /// Достигнут ли кворум для записи.
    /// </summary>
    public bool HasWriteQuorum => AvailableCount >= _options.WriteQuorum;

    /// <summary>
    /// Общее количество узлов в наборе.
    /// </summary>
    public int TotalCount
    {
        get
        {
            lock (_lock)
            {
                return (_primary != null ? 1 : 0) + _replicas.Count;
            }
        }
    }

    /// <summary>
    /// Событие при смене primary.
    /// </summary>
    public event EventHandler<PrimaryChangedEventArgs>? PrimaryChanged;

    public ReplicaSet(int shardId, ReplicationOptions options, ILogger? logger = null)
    {
        ShardId = shardId;
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger;
    }

    /// <summary>
    /// Инициализирует набор реплик из конфигурации.
    /// </summary>
    public void Initialize(ReplicaSetConfig config)
    {
        lock (_lock)
        {
            _primary = new ReplicaInfo
            {
                ShardId = ShardId,
                ReplicaIndex = 0,
                BaseUrl = config.Primary,
                IsPrimary = true,
                HealthStatus = ReplicaHealthStatus.Unknown,
                SyncState = ReplicaSyncState.Unknown
            };

            _replicas.Clear();
            for (int i = 0; i < config.Replicas.Count; i++)
            {
                _replicas.Add(new ReplicaInfo
                {
                    ShardId = ShardId,
                    ReplicaIndex = i + 1,
                    BaseUrl = config.Replicas[i],
                    IsPrimary = false,
                    HealthStatus = ReplicaHealthStatus.Unknown,
                    SyncState = ReplicaSyncState.Unknown
                });
            }

            _logger?.LogInformation(
                "ReplicaSet[{ShardId}] initialized with primary {Primary} and {ReplicaCount} replicas",
                ShardId, config.Primary, config.Replicas.Count);
        }
    }

    /// <summary>
    /// Инициализирует набор реплик для локального режима.
    /// </summary>
    public void InitializeLocal(int replicaCount)
    {
        lock (_lock)
        {
            _primary = new ReplicaInfo
            {
                ShardId = ShardId,
                ReplicaIndex = 0,
                BaseUrl = $"local://shard-{ShardId}-primary",
                IsPrimary = true,
                IsLocal = true,
                HealthStatus = ReplicaHealthStatus.Healthy,
                SyncState = ReplicaSyncState.InSync
            };

            _replicas.Clear();
            for (int i = 0; i < replicaCount; i++)
            {
                _replicas.Add(new ReplicaInfo
                {
                    ShardId = ShardId,
                    ReplicaIndex = i + 1,
                    BaseUrl = $"local://shard-{ShardId}-replica-{i + 1}",
                    IsPrimary = false,
                    IsLocal = true,
                    HealthStatus = ReplicaHealthStatus.Healthy,
                    SyncState = ReplicaSyncState.InSync
                });
            }

            _logger?.LogInformation(
                "ReplicaSet[{ShardId}] initialized in local mode with {ReplicaCount} replicas",
                ShardId, replicaCount);
        }
    }

    /// <summary>
    /// Обновляет состояние здоровья реплики.
    /// </summary>
    public void UpdateHealth(string replicaId, ReplicaHealthStatus status, long? seq = null)
    {
        lock (_lock)
        {
            var replica = FindReplica(replicaId);
            if (replica == null) return;

            var oldStatus = replica.HealthStatus;
            replica.HealthStatus = status;
            replica.LastHealthCheck = DateTimeOffset.UtcNow;

            if (status == ReplicaHealthStatus.Healthy)
            {
                replica.FailedHealthChecks = 0;
                if (seq.HasValue)
                {
                    replica.LastSeq = seq.Value;
                    UpdateSyncState(replica);
                }
            }
            else if (status == ReplicaHealthStatus.Unhealthy)
            {
                replica.FailedHealthChecks++;
                if (replica.FailedHealthChecks >= _options.UnhealthyThreshold)
                {
                    replica.SyncState = ReplicaSyncState.Offline;
                }
            }

            if (oldStatus != status)
            {
                _logger?.LogInformation(
                    "Replica {ReplicaId} health changed: {OldStatus} -> {NewStatus}",
                    replicaId, oldStatus, status);
            }
        }
    }

    /// <summary>
    /// Продвигает реплику в primary.
    /// </summary>
    public bool PromoteToPrimary(string replicaId)
    {
        lock (_lock)
        {
            var newPrimary = FindReplica(replicaId);
            if (newPrimary == null || !newPrimary.CanBeElected)
            {
                _logger?.LogWarning(
                    "Cannot promote replica {ReplicaId}: not found or not eligible",
                    replicaId);
                return false;
            }

            var oldPrimary = _primary;

            // Понижаем старый primary до реплики (если он ещё доступен)
            if (oldPrimary != null)
            {
                oldPrimary.IsPrimary = false;
                if (!_replicas.Contains(oldPrimary))
                {
                    _replicas.Add(oldPrimary);
                }
            }

            // Удаляем новый primary из списка реплик
            _replicas.Remove(newPrimary);

            // Продвигаем новый primary
            newPrimary.IsPrimary = true;
            newPrimary.PromotedAt = DateTimeOffset.UtcNow;
            _primary = newPrimary;

            _logger?.LogInformation(
                "ReplicaSet[{ShardId}] promoted {NewPrimary} to primary (was: {OldPrimary})",
                ShardId, replicaId, oldPrimary?.UniqueId ?? "none");

            PrimaryChanged?.Invoke(this, new PrimaryChangedEventArgs(oldPrimary, newPrimary));

            return true;
        }
    }

    /// <summary>
    /// Выбирает лучшую реплику для продвижения в primary.
    /// Критерий: наивысший seq среди здоровых и синхронизированных реплик.
    /// </summary>
    public ReplicaInfo? ElectNewPrimary()
    {
        lock (_lock)
        {
            var candidates = _replicas
                .Where(r => r.CanBeElected)
                .OrderByDescending(r => r.LastSeq)
                .ThenBy(r => r.ReplicaIndex) // При равном seq выбираем с меньшим индексом
                .ToList();

            if (candidates.Count == 0)
            {
                _logger?.LogWarning(
                    "ReplicaSet[{ShardId}] no eligible candidates for primary election",
                    ShardId);
                return null;
            }

            var elected = candidates[0];
            _logger?.LogInformation(
                "ReplicaSet[{ShardId}] elected {Replica} as new primary candidate (seq={Seq})",
                ShardId, elected.UniqueId, elected.LastSeq);

            return elected;
        }
    }

    /// <summary>
    /// Возвращает реплики, доступные для кворумной записи.
    /// </summary>
    public IReadOnlyList<ReplicaInfo> GetWriteTargets()
    {
        lock (_lock)
        {
            var targets = new List<ReplicaInfo>();
            if (_primary?.IsAvailable == true)
                targets.Add(_primary);

            targets.AddRange(_replicas.Where(r => r.IsAvailable));
            return targets;
        }
    }

    /// <summary>
    /// Возвращает реплики, доступные для чтения.
    /// </summary>
    public IReadOnlyList<ReplicaInfo> GetReadTargets()
    {
        lock (_lock)
        {
            if (!_options.ReadLoadBalancing)
            {
                // Только primary
                if (_primary?.IsAvailable == true)
                    return new[] { _primary };
                return Array.Empty<ReplicaInfo>();
            }

            // Primary + синхронизированные реплики
            var targets = new List<ReplicaInfo>();
            if (_primary?.IsAvailable == true)
                targets.Add(_primary);

            targets.AddRange(_replicas.Where(r => r.IsAvailable && r.SyncState == ReplicaSyncState.InSync));
            return targets;
        }
    }

    /// <summary>
    /// Помечает primary как недоступный.
    /// </summary>
    public void MarkPrimaryDown()
    {
        lock (_lock)
        {
            if (_primary != null)
            {
                _primary.HealthStatus = ReplicaHealthStatus.Unhealthy;
                _primary.SyncState = ReplicaSyncState.Offline;
                _logger?.LogWarning("ReplicaSet[{ShardId}] primary marked as down", ShardId);
            }
        }
    }

    private ReplicaInfo? FindReplica(string replicaId)
    {
        if (_primary?.UniqueId == replicaId)
            return _primary;

        return _replicas.FirstOrDefault(r => r.UniqueId == replicaId);
    }

    private void UpdateSyncState(ReplicaInfo replica)
    {
        if (_primary == null || replica.IsPrimary) return;

        var lag = _primary.LastSeq - replica.LastSeq;
        if (lag <= 0)
        {
            replica.SyncState = ReplicaSyncState.InSync;
            replica.ReplicationLagMs = 0;
        }
        else if (lag < 100) // Порог для InSync
        {
            replica.SyncState = ReplicaSyncState.InSync;
            replica.ReplicationLagMs = lag * 10; // Примерная оценка
        }
        else
        {
            replica.SyncState = ReplicaSyncState.Lagging;
            replica.ReplicationLagMs = lag * 10;
        }
    }
}

/// <summary>
/// Аргументы события смены primary.
/// </summary>
public sealed class PrimaryChangedEventArgs : EventArgs
{
    public ReplicaInfo? OldPrimary { get; }
    public ReplicaInfo NewPrimary { get; }

    public PrimaryChangedEventArgs(ReplicaInfo? oldPrimary, ReplicaInfo newPrimary)
    {
        OldPrimary = oldPrimary;
        NewPrimary = newPrimary ?? throw new ArgumentNullException(nameof(newPrimary));
    }
}

