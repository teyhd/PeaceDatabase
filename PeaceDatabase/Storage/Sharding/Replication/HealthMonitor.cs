using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Фоновый сервис для мониторинга здоровья реплик.
/// Периодически проверяет доступность всех реплик и инициирует failover при необходимости.
/// </summary>
public sealed class HealthMonitor : BackgroundService
{
    private readonly ILogger<HealthMonitor> _logger;
    private readonly ReplicationOptions _options;
    private readonly ReplicationCoordinator _coordinator;
    private readonly Func<ReplicaInfo, IReplicaClient> _clientFactory;
    private readonly Dictionary<string, IReplicaClient> _clients = new();
    private readonly object _lock = new();

    /// <summary>
    /// Событие при обнаружении сбоя primary.
    /// </summary>
    public event EventHandler<PrimaryDownEventArgs>? PrimaryDown;

    /// <summary>
    /// Событие при восстановлении узла.
    /// </summary>
    public event EventHandler<ReplicaRecoveredEventArgs>? ReplicaRecovered;

    public HealthMonitor(
        ReplicationOptions options,
        ReplicationCoordinator coordinator,
        Func<ReplicaInfo, IReplicaClient> clientFactory,
        ILogger<HealthMonitor> logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "HealthMonitor started with interval {IntervalMs}ms",
            _options.HealthCheckIntervalMs);

        // Ждём немного перед первой проверкой, чтобы система инициализировалась
        await Task.Delay(1000, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckAllReplicasAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during health check cycle");
            }

            await Task.Delay(_options.HealthCheckIntervalMs, stoppingToken);
        }

        _logger.LogInformation("HealthMonitor stopped");
    }

    private async Task CheckAllReplicasAsync(CancellationToken ct)
    {
        var replicaSets = _coordinator.GetAllReplicaSets();
        var tasks = new List<Task>();

        foreach (var replicaSet in replicaSets)
        {
            foreach (var replica in replicaSet.AllReplicas)
            {
                tasks.Add(CheckReplicaAsync(replicaSet, replica, ct));
            }
        }

        await Task.WhenAll(tasks);
    }

    private async Task CheckReplicaAsync(ReplicaSet replicaSet, ReplicaInfo replica, CancellationToken ct)
    {
        var client = GetOrCreateClient(replica);
        var wasHealthy = replica.HealthStatus == ReplicaHealthStatus.Healthy;
        var wasPrimary = replica.IsPrimary;

        try
        {
            var state = await client.GetReplicationStateAsync(ct);

            if (state.IsHealthy)
            {
                replicaSet.UpdateHealth(replica.UniqueId, ReplicaHealthStatus.Healthy, state.Seq);

                // Если узел восстановился, уведомляем
                if (!wasHealthy)
                {
                    _logger.LogInformation(
                        "Replica {ReplicaId} recovered (seq={Seq})",
                        replica.UniqueId, state.Seq);

                    ReplicaRecovered?.Invoke(this, new ReplicaRecoveredEventArgs(replica));
                }
            }
            else
            {
                HandleUnhealthyReplica(replicaSet, replica, wasPrimary);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Health check failed for replica {ReplicaId}: {Error}",
                replica.UniqueId, ex.Message);

            HandleUnhealthyReplica(replicaSet, replica, wasPrimary);
        }
    }

    private void HandleUnhealthyReplica(ReplicaSet replicaSet, ReplicaInfo replica, bool wasPrimary)
    {
        replicaSet.UpdateHealth(replica.UniqueId, ReplicaHealthStatus.Unhealthy);

        // Если это primary и он стал недоступен после threshold проверок
        if (wasPrimary && replica.FailedHealthChecks >= _options.UnhealthyThreshold)
        {
            _logger.LogWarning(
                "Primary {ReplicaId} is down after {FailedChecks} failed health checks, initiating failover",
                replica.UniqueId, replica.FailedHealthChecks);

            replicaSet.MarkPrimaryDown();
            PrimaryDown?.Invoke(this, new PrimaryDownEventArgs(replicaSet.ShardId, replica));
        }
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
    /// Принудительно проверяет конкретную реплику.
    /// </summary>
    public async Task<bool> CheckReplicaNowAsync(int shardId, string replicaId, CancellationToken ct = default)
    {
        var replicaSet = _coordinator.GetReplicaSet(shardId);
        if (replicaSet == null) return false;

        var replica = replicaSet.AllReplicas.FirstOrDefault(r => r.UniqueId == replicaId);
        if (replica == null) return false;

        await CheckReplicaAsync(replicaSet, replica, ct);
        return replica.HealthStatus == ReplicaHealthStatus.Healthy;
    }

    public override void Dispose()
    {
        lock (_lock)
        {
            foreach (var client in _clients.Values)
            {
                client.Dispose();
            }
            _clients.Clear();
        }

        base.Dispose();
    }
}

/// <summary>
/// Аргументы события падения primary.
/// </summary>
public sealed class PrimaryDownEventArgs : EventArgs
{
    public int ShardId { get; }
    public ReplicaInfo DownedPrimary { get; }

    public PrimaryDownEventArgs(int shardId, ReplicaInfo downedPrimary)
    {
        ShardId = shardId;
        DownedPrimary = downedPrimary ?? throw new ArgumentNullException(nameof(downedPrimary));
    }
}

/// <summary>
/// Аргументы события восстановления реплики.
/// </summary>
public sealed class ReplicaRecoveredEventArgs : EventArgs
{
    public ReplicaInfo RecoveredReplica { get; }

    public ReplicaRecoveredEventArgs(ReplicaInfo recoveredReplica)
    {
        RecoveredReplica = recoveredReplica ?? throw new ArgumentNullException(nameof(recoveredReplica));
    }
}

