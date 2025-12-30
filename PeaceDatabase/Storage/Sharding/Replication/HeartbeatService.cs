using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Фоновый сервис для отправки heartbeat (лидер) и проверки election timeout (follower).
/// Реализует ключевые механизмы Raft для поддержания лидерства.
/// </summary>
public sealed class HeartbeatService : BackgroundService
{
    private readonly ILogger<HeartbeatService> _logger;
    private readonly ReplicationOptions _options;
    private readonly ReplicationCoordinator _coordinator;
    private readonly Func<ReplicaInfo, IReplicaClient> _clientFactory;
    private readonly RaftState _raftState;
    private readonly RaftElection _raftElection;

    private int _currentElectionTimeout;
    private readonly object _lock = new();

    /// <summary>
    /// Событие при старте выборов.
    /// </summary>
    public event EventHandler<ElectionStartedEventArgs>? ElectionStarted;

    /// <summary>
    /// Событие при завершении выборов.
    /// </summary>
    public event EventHandler<ElectionCompletedEventArgs>? ElectionCompleted;

    public HeartbeatService(
        ReplicationOptions options,
        ReplicationCoordinator coordinator,
        RaftState raftState,
        Func<ReplicaInfo, IReplicaClient> clientFactory,
        ILogger<HeartbeatService> logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _raftState = raftState ?? throw new ArgumentNullException(nameof(raftState));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _raftElection = new RaftElection(options, raftState, clientFactory, logger as ILogger<RaftElection>);

        // Инициализируем случайный election timeout
        ResetElectionTimeout();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.RaftEnabled)
        {
            _logger.LogInformation("Raft is disabled, HeartbeatService will not run");
            return;
        }

        _logger.LogInformation(
            "HeartbeatService started (heartbeat={HeartbeatMs}ms, election={ElectionMin}-{ElectionMax}ms)",
            _options.HeartbeatIntervalMs,
            _options.ElectionTimeoutMinMs,
            _options.ElectionTimeoutMaxMs);

        // Небольшая задержка перед стартом
        await Task.Delay(1000, stoppingToken);

        // Term synchronization: query peers for current term and adopt highest
        await SyncTermWithPeersAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_raftState.IsLeader)
                {
                    await SendHeartbeatsAsync(stoppingToken);
                }
                else
                {
                    await CheckElectionTimeoutAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in HeartbeatService cycle");
            }

            // Интервал между итерациями = heartbeat interval
            await Task.Delay(_options.HeartbeatIntervalMs, stoppingToken);
        }

        _logger.LogInformation("HeartbeatService stopped");
    }

    /// <summary>
    /// Отправляет heartbeat всем follower'ам (когда мы лидер).
    /// </summary>
    private async Task SendHeartbeatsAsync(CancellationToken ct)
    {
        var replicaSets = _coordinator.GetAllReplicaSets();
        var heartbeatTasks = new List<Task>();

        foreach (var replicaSet in replicaSets)
        {
            // Проверяем, являемся ли мы лидером этого шарда
            if (replicaSet.LocalReplicaIndex != 0 && !_raftState.IsLeader)
                continue;

            foreach (var replica in replicaSet.Replicas)
            {
                // Пропускаем себя
                if (replica.ReplicaIndex == replicaSet.LocalReplicaIndex)
                    continue;

                // Пропускаем очевидно недоступные
                if (replica.HealthStatus == ReplicaHealthStatus.Unhealthy)
                    continue;

                heartbeatTasks.Add(SendHeartbeatToReplicaAsync(replica, ct));
            }
        }

        if (heartbeatTasks.Count > 0)
        {
            await Task.WhenAll(heartbeatTasks);
        }
    }

    private async Task SendHeartbeatToReplicaAsync(ReplicaInfo replica, CancellationToken ct)
    {
        try
        {
            using var client = _clientFactory(replica);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_options.HeartbeatIntervalMs * 2); // Timeout = 2x heartbeat interval

            var snapshot = _raftState.GetSnapshot();
            var response = await client.SendHeartbeatAsync(
                snapshot.CurrentTerm,
                snapshot.NodeId,
                null, // leaderUrl - можно добавить позже
                cts.Token);

            // Если follower имеет больший терм - становимся follower
            if (response.Term > snapshot.CurrentTerm)
            {
                _logger.LogWarning(
                    "Replica {ReplicaId} has higher term {Term}, stepping down from leadership",
                    replica.UniqueId, response.Term);

                _raftState.BecomeFollower(response.Term);
            }
            else if (!response.Success)
            {
                _logger.LogDebug(
                    "Heartbeat to {ReplicaId} rejected (term mismatch?)",
                    replica.UniqueId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                "Heartbeat to {ReplicaId} failed: {Error}",
                replica.UniqueId, ex.Message);
        }
    }

    /// <summary>
    /// Проверяет election timeout (когда мы follower).
    /// </summary>
    private async Task CheckElectionTimeoutAsync(CancellationToken ct)
    {
        // Проверяем, истёк ли election timeout
        if (!_raftState.IsElectionTimeoutElapsed(_currentElectionTimeout))
            return;

        // Начинаем выборы
        _logger.LogInformation(
            "Election timeout elapsed ({Timeout}ms), starting election",
            _currentElectionTimeout);

        ElectionStarted?.Invoke(this, new ElectionStartedEventArgs(_raftState.CurrentTerm + 1));

        // Получаем ReplicaSet для нашего шарда
        var replicaSets = _coordinator.GetAllReplicaSets();
        
        foreach (var replicaSet in replicaSets)
        {
            // Проверяем, что это наш локальный шард
            if (replicaSet.LocalReplicaIndex == null)
                continue;

            // Получаем наш текущий seq
            long mySeq = GetLocalSeq(replicaSet);

            var elected = await _raftElection.StartElectionAsync(replicaSet, mySeq, ct);

            ElectionCompleted?.Invoke(this, new ElectionCompletedEventArgs(
                _raftState.CurrentTerm,
                elected));

            // Сбрасываем election timeout после выборов
            ResetElectionTimeout();
        }
    }

    private long GetLocalSeq(ReplicaSet replicaSet)
    {
        // Получаем seq локальной реплики
        // В реальной реализации это должно быть из локального хранилища
        return 0; // Упрощение для MVP
    }

    private void ResetElectionTimeout()
    {
        lock (_lock)
        {
            _currentElectionTimeout = _raftState.GetRandomElectionTimeout(
                _options.ElectionTimeoutMinMs,
                _options.ElectionTimeoutMaxMs);

            _raftState.ResetHeartbeat();

            _logger.LogDebug("Election timeout reset to {Timeout}ms", _currentElectionTimeout);
        }
    }

    /// <summary>
    /// Synchronizes term with peers on startup.
    /// Queries all peers for their current term and adopts the highest.
    /// This ensures rejoining nodes don't claim leadership with stale terms.
    /// </summary>
    private async Task SyncTermWithPeersAsync(CancellationToken ct)
    {
        _logger.LogInformation("Synchronizing term with peers...");

        var replicaSets = _coordinator.GetAllReplicaSets();
        long highestTerm = _raftState.CurrentTerm;
        string? currentLeaderId = null;

        foreach (var replicaSet in replicaSets)
        {
            foreach (var replica in replicaSet.Replicas)
            {
                // Skip self
                if (replica.ReplicaIndex == replicaSet.LocalReplicaIndex)
                    continue;

                try
                {
                    var client = _clientFactory(replica);
                    
                    // Send a heartbeat request to check peer's state
                    // The response will include the peer's term
                    var response = await client.SendHeartbeatAsync(
                        _raftState.CurrentTerm,
                        _raftState.NodeId,
                        null, // No leader URL during sync
                        ct);

                    if (response.Term > highestTerm)
                    {
                        highestTerm = response.Term;
                        _logger.LogInformation(
                            "Found higher term {Term} from peer {ReplicaId}",
                            response.Term, replica.UniqueId);
                    }

                    // If peer is a leader with higher term, note it
                    if (response.Success && response.Term >= highestTerm)
                    {
                        currentLeaderId = response.LeaderId;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(
                        "Could not sync term with {ReplicaId}: {Error}",
                        replica.UniqueId, ex.Message);
                }
            }
        }

        // Update to highest term if needed
        if (highestTerm > _raftState.CurrentTerm)
        {
            _logger.LogInformation(
                "Updating term from {OldTerm} to {NewTerm} after sync",
                _raftState.CurrentTerm, highestTerm);
            
            _raftState.UpdateTerm(highestTerm);
            
            if (!string.IsNullOrEmpty(currentLeaderId))
            {
                _raftState.SetLeaderId(currentLeaderId);
            }
        }
        else
        {
            _logger.LogInformation(
                "Term sync complete, current term {Term} is up to date",
                _raftState.CurrentTerm);
        }

        // Reset election timeout after sync
        ResetElectionTimeout();
    }
}

/// <summary>
/// Аргументы события начала выборов.
/// </summary>
public sealed class ElectionStartedEventArgs : EventArgs
{
    public long Term { get; }

    public ElectionStartedEventArgs(long term)
    {
        Term = term;
    }
}

/// <summary>
/// Аргументы события завершения выборов.
/// </summary>
public sealed class ElectionCompletedEventArgs : EventArgs
{
    public long Term { get; }
    public bool Elected { get; }

    public ElectionCompletedEventArgs(long term, bool elected)
    {
        Term = term;
        Elected = elected;
    }
}

