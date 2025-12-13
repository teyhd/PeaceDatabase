using Microsoft.Extensions.Logging;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Алгоритм выбора лидера (primary) для набора реплик.
/// Использует seq-based election: реплика с наивысшим seq становится primary.
/// </summary>
public sealed class LeaderElection
{
    private readonly ILogger<LeaderElection>? _logger;
    private readonly ReplicationOptions _options;
    private readonly Func<ReplicaInfo, IReplicaClient> _clientFactory;

    public LeaderElection(
        ReplicationOptions options,
        Func<ReplicaInfo, IReplicaClient> clientFactory,
        ILogger<LeaderElection>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _logger = logger;
    }

    /// <summary>
    /// Проводит выборы нового primary для набора реплик.
    /// </summary>
    /// <param name="replicaSet">Набор реплик</param>
    /// <param name="ct">Токен отмены</param>
    /// <returns>Избранная реплика или null, если выборы не удались</returns>
    public async Task<ReplicaInfo?> ElectPrimaryAsync(ReplicaSet replicaSet, CancellationToken ct = default)
    {
        _logger?.LogInformation(
            "Starting leader election for shard {ShardId}",
            replicaSet.ShardId);

        // Собираем состояние всех доступных реплик
        var candidates = await GetCandidateStatesAsync(replicaSet, ct);

        if (candidates.Count == 0)
        {
            _logger?.LogError(
                "Leader election failed for shard {ShardId}: no healthy candidates",
                replicaSet.ShardId);
            return null;
        }

        // Выбираем реплику с максимальным seq
        var elected = SelectBestCandidate(candidates);

        _logger?.LogInformation(
            "Leader election completed for shard {ShardId}: {Elected} with seq={Seq}",
            replicaSet.ShardId, elected.Replica.UniqueId, elected.Seq);

        // Продвигаем выбранную реплику в primary
        if (replicaSet.PromoteToPrimary(elected.Replica.UniqueId))
        {
            // Уведомляем выбранную реплику о promotion
            await NotifyPromotionAsync(elected.Replica, ct);
            return elected.Replica;
        }

        _logger?.LogError(
            "Failed to promote {Replica} to primary",
            elected.Replica.UniqueId);
        return null;
    }

    /// <summary>
    /// Собирает информацию о состоянии всех кандидатов.
    /// </summary>
    private async Task<List<CandidateState>> GetCandidateStatesAsync(ReplicaSet replicaSet, CancellationToken ct)
    {
        var candidates = new List<CandidateState>();
        var tasks = new List<Task<CandidateState?>>();

        foreach (var replica in replicaSet.Replicas)
        {
            // Пропускаем явно недоступные реплики
            if (replica.HealthStatus == ReplicaHealthStatus.Unhealthy)
                continue;

            tasks.Add(GetCandidateStateAsync(replica, ct));
        }

        var results = await Task.WhenAll(tasks);

        foreach (var result in results)
        {
            if (result != null)
                candidates.Add(result);
        }

        return candidates;
    }

    private async Task<CandidateState?> GetCandidateStateAsync(ReplicaInfo replica, CancellationToken ct)
    {
        try
        {
            using var client = _clientFactory(replica);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_options.FailoverTimeoutMs);

            var state = await client.GetReplicationStateAsync(cts.Token);

            if (state.IsHealthy)
            {
                return new CandidateState(replica, state.Seq, state.WalPosition);
            }
        }
        catch (OperationCanceledException)
        {
            _logger?.LogWarning(
                "Timeout getting state from replica {ReplicaId}",
                replica.UniqueId);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                "Error getting state from replica {ReplicaId}: {Error}",
                replica.UniqueId, ex.Message);
        }

        return null;
    }

    /// <summary>
    /// Выбирает лучшего кандидата на основе seq и других критериев.
    /// </summary>
    private CandidateState SelectBestCandidate(List<CandidateState> candidates)
    {
        // Сортировка: 
        // 1. По убыванию seq (главный критерий - актуальность данных)
        // 2. По возрастанию ReplicaIndex (при равном seq предпочитаем меньший индекс)
        return candidates
            .OrderByDescending(c => c.Seq)
            .ThenBy(c => c.Replica.ReplicaIndex)
            .First();
    }

    /// <summary>
    /// Уведомляет реплику о её продвижении в primary.
    /// </summary>
    private async Task NotifyPromotionAsync(ReplicaInfo replica, CancellationToken ct)
    {
        try
        {
            using var client = _clientFactory(replica);
            await client.PromoteAsync(ct);

            _logger?.LogInformation(
                "Replica {ReplicaId} notified about promotion",
                replica.UniqueId);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                "Failed to notify replica {ReplicaId} about promotion: {Error}",
                replica.UniqueId, ex.Message);
            // Продолжаем, даже если уведомление не удалось
        }
    }

    /// <summary>
    /// Проверяет, нужно ли провести перевыборы (например, если текущий primary отстаёт).
    /// </summary>
    public async Task<bool> ShouldReelectAsync(ReplicaSet replicaSet, CancellationToken ct = default)
    {
        var primary = replicaSet.Primary;
        if (primary == null || primary.HealthStatus != ReplicaHealthStatus.Healthy)
            return true;

        // Проверяем, не отстаёт ли primary
        var candidates = await GetCandidateStatesAsync(replicaSet, ct);
        if (candidates.Count == 0)
            return false;

        var maxSeq = candidates.Max(c => c.Seq);
        var primarySeq = primary.LastSeq;

        // Если есть реплика, значительно опережающая primary, возможно стоит переизбрать
        // (это может произойти после split-brain или при проблемах с репликацией)
        var lag = maxSeq - primarySeq;
        if (lag > 1000) // Порог для перевыборов
        {
            _logger?.LogWarning(
                "Primary {PrimaryId} is lagging by {Lag} seq, considering re-election",
                primary.UniqueId, lag);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Состояние кандидата для выборов.
    /// </summary>
    private sealed record CandidateState(ReplicaInfo Replica, long Seq, string? WalPosition);
}

