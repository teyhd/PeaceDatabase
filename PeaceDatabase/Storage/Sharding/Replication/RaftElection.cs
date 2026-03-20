using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Алгоритм выборов лидера на основе Raft.
/// Использует терм-based голосование с RequestVote RPC.
/// </summary>
public sealed class RaftElection
{
    private readonly ILogger<RaftElection>? _logger;
    private readonly ReplicationOptions _options;
    private readonly Func<ReplicaInfo, IReplicaClient> _clientFactory;
    private readonly RaftState _raftState;

    public RaftElection(
        ReplicationOptions options,
        RaftState raftState,
        Func<ReplicaInfo, IReplicaClient> clientFactory,
        ILogger<RaftElection>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _raftState = raftState ?? throw new ArgumentNullException(nameof(raftState));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger;
    }

    /// <summary>
    /// Проводит выборы лидера для данного набора реплик.
    /// </summary>
    /// <param name="replicaSet">Набор реплик</param>
    /// <param name="mySeq">Наш текущий seq (для сравнения логов)</param>
    /// <param name="ct">Токен отмены</param>
    /// <returns>true, если узел стал лидером</returns>
    public async Task<bool> StartElectionAsync(ReplicaSet replicaSet, long mySeq, CancellationToken ct = default)
    {
        // Начинаем выборы - увеличиваем терм и голосуем за себя
        var newTerm = _raftState.StartElection();
        var myId = $"{replicaSet.ShardId}-{replicaSet.LocalReplicaIndex}";

        _logger?.LogInformation(
            "Starting election for shard {ShardId}, term={Term}, candidate={CandidateId}",
            replicaSet.ShardId, newTerm, myId);

        // Считаем голоса (мы уже проголосовали за себя)
        int votesReceived = 1;
        int totalVoters = replicaSet.AllReplicas.Count();
        int majority = (totalVoters / 2) + 1;

        // Отправляем RequestVote всем остальным репликам
        var voteRequests = new List<Task<(ReplicaInfo replica, VoteResponse? response)>>();

        foreach (var replica in replicaSet.Replicas)
        {
            // Пропускаем себя
            if (replica.ReplicaIndex == replicaSet.LocalReplicaIndex)
                continue;

            // Пропускаем недоступные реплики
            if (replica.HealthStatus == ReplicaHealthStatus.Unhealthy)
                continue;

            voteRequests.Add(RequestVoteFromReplicaAsync(replica, newTerm, myId, mySeq, ct));
        }

        // Ждём ответов
        var responses = await Task.WhenAll(voteRequests);

        foreach (var (replica, response) in responses)
        {
            if (response == null)
            {
                _logger?.LogDebug("No response from {ReplicaId}", replica.UniqueId);
                continue;
            }

            // Если кто-то имеет больший терм - становимся follower
            if (response.Term > newTerm)
            {
                _logger?.LogInformation(
                    "Discovered higher term {Term} from {ReplicaId}, stepping down",
                    response.Term, replica.UniqueId);
                
                _raftState.BecomeFollower(response.Term);
                return false;
            }

            if (response.VoteGranted)
            {
                votesReceived++;
                _logger?.LogDebug(
                    "Vote granted from {ReplicaId}, votes={Votes}/{Majority}",
                    replica.UniqueId, votesReceived, majority);
            }
            else
            {
                _logger?.LogDebug(
                    "Vote denied from {ReplicaId}",
                    replica.UniqueId);
            }
        }

        // Проверяем, набрали ли большинство
        if (votesReceived >= majority)
        {
            _raftState.BecomeLeader();
            
            _logger?.LogInformation(
                "Election won! Node is now leader for shard {ShardId}, term={Term}, votes={Votes}/{Total}",
                replicaSet.ShardId, newTerm, votesReceived, totalVoters);

            // Уведомляем всех о новом лидере (через promote)
            await NotifyLeadershipAsync(replicaSet, ct);
            
            return true;
        }
        else
        {
            _logger?.LogInformation(
                "Election failed for shard {ShardId}, term={Term}, votes={Votes}/{Majority}",
                replicaSet.ShardId, newTerm, votesReceived, majority);

            // Возвращаемся в follower и ждём следующих выборов
            _raftState.BecomeFollower(newTerm);
            return false;
        }
    }

    private async Task<(ReplicaInfo replica, VoteResponse? response)> RequestVoteFromReplicaAsync(
        ReplicaInfo replica,
        long term,
        string candidateId,
        long lastSeq,
        CancellationToken ct)
    {
        try
        {
            using var client = _clientFactory(replica);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_options.FailoverTimeoutMs);

            var response = await client.RequestVoteAsync(term, candidateId, lastSeq, cts.Token);
            return (replica, response);
        }
        catch (OperationCanceledException)
        {
            _logger?.LogDebug("RequestVote to {ReplicaId} timed out", replica.UniqueId);
            return (replica, null);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug("RequestVote to {ReplicaId} failed: {Error}", replica.UniqueId, ex.Message);
            return (replica, null);
        }
    }

    private async Task NotifyLeadershipAsync(ReplicaSet replicaSet, CancellationToken ct)
    {
        var tasks = new List<Task>();

        foreach (var replica in replicaSet.Replicas)
        {
            // Пропускаем себя
            if (replica.ReplicaIndex == replicaSet.LocalReplicaIndex)
                continue;

            tasks.Add(NotifyReplicaAsync(replica, ct));
        }

        await Task.WhenAll(tasks);
    }

    private async Task NotifyReplicaAsync(ReplicaInfo replica, CancellationToken ct)
    {
        try
        {
            using var client = _clientFactory(replica);
            // Отправляем heartbeat чтобы уведомить о новом лидере
            await client.SendHeartbeatAsync(
                _raftState.CurrentTerm,
                _raftState.GetSnapshot().NodeId,
                null, // leaderUrl будет установлен позже
                ct);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug("Failed to notify {ReplicaId} about new leadership: {Error}", 
                replica.UniqueId, ex.Message);
        }
    }

    /// <summary>
    /// Проверяет, следует ли начать выборы (истёк election timeout).
    /// </summary>
    public bool ShouldStartElection()
    {
        // Не начинаем выборы если мы уже лидер
        if (_raftState.IsLeader)
            return false;

        // Генерируем случайный таймаут в диапазоне
        var timeout = _raftState.GetRandomElectionTimeout(
            _options.ElectionTimeoutMinMs,
            _options.ElectionTimeoutMaxMs);

        return _raftState.IsElectionTimeoutElapsed(timeout);
    }
}

