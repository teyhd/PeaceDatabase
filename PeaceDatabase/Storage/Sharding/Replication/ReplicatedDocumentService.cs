using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;
using PeaceDatabase.Storage.Sharding.Routing;

namespace PeaceDatabase.Storage.Sharding.Replication;

/// <summary>
/// Документный сервис с поддержкой репликации.
/// Реализует кворумные записи, failover и scatter-gather для запросов.
/// </summary>
public sealed class ReplicatedDocumentService : IDocumentService, IDisposable
{
    private readonly ShardingOptions _shardingOptions;
    private readonly IShardRouter _router;
    private readonly ReplicationCoordinator _coordinator;
    private readonly ILogger<ReplicatedDocumentService>? _logger;
    private readonly Random _random = new();
    private bool _disposed;

    private ReplicationOptions ReplicationOptions => _shardingOptions.Replication;

    public ReplicatedDocumentService(
        ShardingOptions shardingOptions,
        IShardRouter router,
        ReplicationCoordinator coordinator,
        ILogger<ReplicatedDocumentService>? logger = null)
    {
        _shardingOptions = shardingOptions ?? throw new ArgumentNullException(nameof(shardingOptions));
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _logger = logger;

        // Инициализируем координатор
        _coordinator.Initialize();

        _logger?.LogInformation(
            "ReplicatedDocumentService initialized with WriteQuorum={WriteQuorum}, ReadQuorum={ReadQuorum}",
            ReplicationOptions.WriteQuorum, ReplicationOptions.ReadQuorum);
    }

    #region Database Operations (Broadcast)

    public (bool Ok, string? Error) CreateDb(string db)
    {
        return CreateDbAsync(db).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> CreateDbAsync(string db)
    {
        var replicaSets = _coordinator.GetAllReplicaSets();
        var tasks = new List<Task<(bool Ok, string? Error)>>();

        foreach (var replicaSet in replicaSets)
        {
            foreach (var replica in replicaSet.AllReplicas.Where(r => r.IsAvailable))
            {
                tasks.Add(CreateDbOnReplicaAsync(replica, db));
            }
        }

        var results = await Task.WhenAll(tasks);
        var errors = results.Where(r => !r.Ok && r.Error != "Database already exists").Select(r => r.Error).ToList();

        if (errors.Count > 0 && !results.Any(r => r.Ok))
        {
            return (false, string.Join("; ", errors));
        }

        return (true, null);
    }

    private async Task<(bool Ok, string? Error)> CreateDbOnReplicaAsync(ReplicaInfo replica, string db)
    {
        try
        {
            var client = GetClient(replica);
            return await client.CreateDbAsync(db);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    public (bool Ok, string? Error) DeleteDb(string db)
    {
        return DeleteDbAsync(db).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> DeleteDbAsync(string db)
    {
        var replicaSets = _coordinator.GetAllReplicaSets();
        var tasks = new List<Task<(bool Ok, string? Error)>>();

        foreach (var replicaSet in replicaSets)
        {
            foreach (var replica in replicaSet.AllReplicas.Where(r => r.IsAvailable))
            {
                tasks.Add(DeleteDbOnReplicaAsync(replica, db));
            }
        }

        var results = await Task.WhenAll(tasks);
        var errors = results.Where(r => !r.Ok).Select(r => r.Error).ToList();

        if (errors.Count > 0 && !results.Any(r => r.Ok))
        {
            return (false, string.Join("; ", errors));
        }

        return (true, null);
    }

    private async Task<(bool Ok, string? Error)> DeleteDbOnReplicaAsync(ReplicaInfo replica, string db)
    {
        try
        {
            var client = GetClient(replica);
            return await client.DeleteDbAsync(db);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    #endregion

    #region Document CRUD (Routed + Quorum)

    public Document? Get(string db, string id, string? rev = null)
    {
        return GetAsync(db, id, rev).GetAwaiter().GetResult();
    }

    private async Task<Document?> GetAsync(string db, string id, string? rev = null)
    {
        var shardId = _router.GetShardId(id);
        var clients = _coordinator.GetReadClients(shardId);

        if (clients.Count == 0)
        {
            _logger?.LogWarning("No available replicas for shard {ShardId}", shardId);
            return null;
        }

        // Выбираем случайную реплику для балансировки нагрузки
        var client = clients[_random.Next(clients.Count)];

        try
        {
            return await client.GetAsync(db, id, rev);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("Failed to get document {Id} from shard {ShardId}: {Error}", id, shardId, ex.Message);
            
            // Пробуем другие реплики
            foreach (var fallback in clients.Where(c => c != client))
            {
                try
                {
                    return await fallback.GetAsync(db, id, rev);
                }
                catch
                {
                    continue;
                }
            }
            return null;
        }
    }

    public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
    {
        return PutAsync(db, doc).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc)
    {
        if (string.IsNullOrWhiteSpace(doc.Id))
        {
            _logger?.LogWarning("Put failed: Missing _id for document in db {Db}", db);
            return (false, null, "Missing _id");
        }

        var shardId = _router.GetShardId(doc.Id);
        _logger?.LogDebug("Put document {DocId} routed to shard {ShardId}", doc.Id, shardId);
        return await QuorumWriteAsync(shardId, db, doc, ReplicationOp.Put);
    }

    public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
    {
        return PostAsync(db, doc).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc)
    {
        // Генерируем ID если не задан
        if (string.IsNullOrWhiteSpace(doc.Id))
            doc.Id = Guid.NewGuid().ToString("N");

        var shardId = _router.GetShardId(doc.Id);
        return await QuorumWriteAsync(shardId, db, doc, ReplicationOp.Post);
    }

    public (bool Ok, string? Error) Delete(string db, string id, string rev)
    {
        return DeleteAsync(db, id, rev).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev)
    {
        var shardId = _router.GetShardId(id);
        var clients = _coordinator.GetWriteClients(shardId);

        if (clients.Count < ReplicationOptions.WriteQuorum)
        {
            return (false, $"Insufficient replicas for quorum (need {ReplicationOptions.WriteQuorum}, have {clients.Count})");
        }

        var tasks = clients.Select(c => c.DeleteAsync(db, id, rev)).ToList();
        var results = await Task.WhenAll(tasks);

        var successCount = results.Count(r => r.Ok);
        if (successCount >= ReplicationOptions.WriteQuorum)
        {
            return (true, null);
        }

        var errors = results.Where(r => !r.Ok).Select(r => r.Error);
        return (false, $"Quorum not reached: {string.Join("; ", errors)}");
    }

    /// <summary>
    /// Выполняет кворумную запись на все реплики шарда.
    /// </summary>
    private async Task<(bool Ok, Document? Doc, string? Error)> QuorumWriteAsync(
        int shardId,
        string db,
        Document doc,
        ReplicationOp op)
    {
        var clients = _coordinator.GetWriteClients(shardId);

        _logger?.LogDebug(
            "QuorumWrite for shard {ShardId}: found {ClientCount} clients, need {WriteQuorum} for quorum",
            shardId, clients.Count, ReplicationOptions.WriteQuorum);

        if (clients.Count < ReplicationOptions.WriteQuorum)
        {
            _logger?.LogWarning(
                "Insufficient replicas for quorum on shard {ShardId}: need {Need}, have {Have}",
                shardId, ReplicationOptions.WriteQuorum, clients.Count);
            return (false, null, $"Insufficient replicas for quorum (need {ReplicationOptions.WriteQuorum}, have {clients.Count})");
        }

        // Запускаем запись параллельно на все реплики
        var tasks = clients.Select(async client =>
        {
            try
            {
                var replicaId = client.ReplicaInfo.UniqueId;
                _logger?.LogDebug("Writing to replica {ReplicaId}", replicaId);
                
                var result = op == ReplicationOp.Put
                    ? await client.PutAsync(db, doc)
                    : await client.PostAsync(db, doc);
                
                if (result.Ok)
                    _logger?.LogDebug("Write succeeded on replica {ReplicaId}", replicaId);
                else
                    _logger?.LogDebug("Write failed on replica {ReplicaId}: {Error}", replicaId, result.Error);
                
                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Exception writing to replica");
                return (false, null as Document, ex.Message);
            }
        }).ToList();

        // Ждём кворум успешных ответов
        var results = new List<(bool Ok, Document? Doc, string? Error)>();
        var completed = 0;
        var successCount = 0;
        Document? resultDoc = null;

        while (completed < tasks.Count && successCount < ReplicationOptions.WriteQuorum)
        {
            var completedTask = await Task.WhenAny(tasks);
            tasks.Remove(completedTask);
            completed++;

            var (resultOk, resultDocTemp, resultError) = await completedTask;
            results.Add((resultOk, resultDocTemp, resultError));

            if (resultOk)
            {
                successCount++;
                resultDoc ??= resultDocTemp;
            }
        }

        if (successCount >= ReplicationOptions.WriteQuorum)
        {
            _logger?.LogDebug(
                "Quorum write succeeded on shard {ShardId}: {SuccessCount}/{TotalCount} replicas",
                shardId, successCount, clients.Count);
            return (true, resultDoc, null);
        }

        var errors = results.Where(r => !r.Item1).Select(r => r.Item3);
        _logger?.LogWarning(
            "Quorum write failed on shard {ShardId}: {SuccessCount}/{WriteQuorum} needed. Errors: {Errors}",
            shardId, successCount, ReplicationOptions.WriteQuorum, string.Join("; ", errors));

        return (false, null, $"Quorum not reached: {string.Join("; ", errors)}");
    }

    #endregion

    #region Scatter-Gather Queries

    public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true)
    {
        return AllDocsAsync(db, skip, limit, includeDeleted).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> AllDocsAsync(string db, int skip, int limit, bool includeDeleted)
    {
        var perShardLimit = skip + limit;
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0)
                    return Array.Empty<Document>();

                // Выбираем случайную реплику для каждого шарда
                var client = clients[_random.Next(clients.Count)];
                try
                {
                    return await client.AllDocsAsync(db, 0, perShardLimit, includeDeleted);
                }
                catch
                {
                    // Пробуем другие реплики
                    foreach (var fallback in clients.Where(c => c != client))
                    {
                        try { return await fallback.AllDocsAsync(db, 0, perShardLimit, includeDeleted); }
                        catch { continue; }
                    }
                    return Array.Empty<Document>();
                }
            })
            .ToList();

        var results = await Task.WhenAll(tasks);

        return results
            .SelectMany(r => r)
            .OrderBy(d => d.Id, StringComparer.Ordinal)
            .Skip(skip)
            .Take(limit);
    }

    public int Seq(string db)
    {
        return SeqAsync(db).GetAwaiter().GetResult();
    }

    private async Task<int> SeqAsync(string db)
    {
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0) return 0;
                var client = clients[_random.Next(clients.Count)];
                try { return await client.SeqAsync(db); }
                catch { return 0; }
            });

        var results = await Task.WhenAll(tasks);
        return results.Max();
    }

    public StatsDto Stats(string db)
    {
        return StatsAsync(db).GetAwaiter().GetResult();
    }

    private async Task<StatsDto> StatsAsync(string db)
    {
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0) return new StatsDto { Db = db };
                var client = clients[_random.Next(clients.Count)];
                try { return await client.StatsAsync(db); }
                catch { return new StatsDto { Db = db }; }
            });

        var results = await Task.WhenAll(tasks);

        return new StatsDto
        {
            Db = db,
            Seq = results.Max(r => r.Seq),
            DocsTotal = results.Sum(r => r.DocsTotal),
            DocsAlive = results.Sum(r => r.DocsAlive),
            DocsDeleted = results.Sum(r => r.DocsDeleted),
            EqIndexFields = results.Max(r => r.EqIndexFields),
            TagIndexCount = results.Max(r => r.TagIndexCount),
            FullTextTokens = results.Sum(r => r.FullTextTokens)
        };
    }

    public IEnumerable<Document> FindByFields(
        string db,
        IDictionary<string, string>? equals = null,
        (string field, double? min, double? max)? numericRange = null,
        int skip = 0,
        int limit = 100)
    {
        return FindByFieldsAsync(db, equals, numericRange, skip, limit).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> FindByFieldsAsync(
        string db,
        IDictionary<string, string>? equals,
        (string field, double? min, double? max)? numericRange,
        int skip,
        int limit)
    {
        var perShardLimit = skip + limit;
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0) return Array.Empty<Document>();
                var client = clients[_random.Next(clients.Count)];
                try { return await client.FindByFieldsAsync(db, equals, numericRange, 0, perShardLimit); }
                catch { return Array.Empty<Document>(); }
            });

        var results = await Task.WhenAll(tasks);
        return results.SelectMany(r => r).OrderBy(d => d.Id, StringComparer.Ordinal).Skip(skip).Take(limit);
    }

    public IEnumerable<Document> FindByTags(
        string db,
        IEnumerable<string>? allOf = null,
        IEnumerable<string>? anyOf = null,
        IEnumerable<string>? noneOf = null,
        int skip = 0,
        int limit = 100)
    {
        return FindByTagsAsync(db, allOf, anyOf, noneOf, skip, limit).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> FindByTagsAsync(
        string db,
        IEnumerable<string>? allOf,
        IEnumerable<string>? anyOf,
        IEnumerable<string>? noneOf,
        int skip,
        int limit)
    {
        var perShardLimit = skip + limit;
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0) return Array.Empty<Document>();
                var client = clients[_random.Next(clients.Count)];
                try { return await client.FindByTagsAsync(db, allOf, anyOf, noneOf, 0, perShardLimit); }
                catch { return Array.Empty<Document>(); }
            });

        var results = await Task.WhenAll(tasks);
        return results.SelectMany(r => r).OrderBy(d => d.Id, StringComparer.Ordinal).Skip(skip).Take(limit);
    }

    public IEnumerable<Document> FullTextSearch(string db, string query, int skip = 0, int limit = 100)
    {
        return FullTextSearchAsync(db, query, skip, limit).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> FullTextSearchAsync(string db, string query, int skip, int limit)
    {
        var perShardLimit = skip + limit;
        var tasks = _coordinator.GetAllReplicaSets()
            .Select(async rs =>
            {
                var clients = _coordinator.GetReadClients(rs.ShardId);
                if (clients.Count == 0) return Array.Empty<Document>();
                var client = clients[_random.Next(clients.Count)];
                try { return await client.FullTextSearchAsync(db, query, 0, perShardLimit); }
                catch { return Array.Empty<Document>(); }
            });

        var results = await Task.WhenAll(tasks);
        return results.SelectMany(r => r).OrderBy(d => d.Id, StringComparer.Ordinal).Skip(skip).Take(limit);
    }

    #endregion

    #region Helpers

    private IReplicaClient GetClient(ReplicaInfo replica)
    {
        // Делегируем получение клиента координатору
        var replicaSet = _coordinator.GetReplicaSet(replica.ShardId);
        if (replicaSet == null)
            throw new InvalidOperationException($"Replica set for shard {replica.ShardId} not found");

        var clients = replica.IsPrimary
            ? _coordinator.GetWriteClients(replica.ShardId)
            : _coordinator.GetReadClients(replica.ShardId);

        return clients.FirstOrDefault(c => ((IReplicaClient)c).ReplicaInfo.UniqueId == replica.UniqueId)
            ?? throw new InvalidOperationException($"Client for replica {replica.UniqueId} not found");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _coordinator.Dispose();
    }

    #endregion
}

