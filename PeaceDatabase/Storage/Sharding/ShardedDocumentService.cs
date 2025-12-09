using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Client;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Discovery;
using PeaceDatabase.Storage.Sharding.Routing;

namespace PeaceDatabase.Storage.Sharding;

/// <summary>
/// Координатор шардирования. Реализует IDocumentService, распределяя запросы по шардам.
/// Использует hash-based маршрутизацию по _id документа.
/// </summary>
public sealed class ShardedDocumentService : IDocumentService, IDisposable
{
    private readonly IShardRouter _router;
    private readonly IShardDiscovery _discovery;
    private readonly ILogger<ShardedDocumentService>? _logger;
    private readonly Dictionary<int, IShardClient> _clients = new();
    private readonly object _clientLock = new();
    private bool _disposed;

    public ShardedDocumentService(
        IShardRouter router,
        IShardDiscovery discovery,
        IEnumerable<IShardClient> clients,
        ILogger<ShardedDocumentService>? logger = null)
    {
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _discovery = discovery ?? throw new ArgumentNullException(nameof(discovery));
        _logger = logger;

        foreach (var client in clients)
        {
            _clients[client.ShardInfo.Id] = client;
        }

        _discovery.ShardsChanged += OnShardsChanged;
        _logger?.LogInformation("ShardedDocumentService initialized with {ShardCount} shards", _clients.Count);
    }

    private void OnShardsChanged(object? sender, ShardListChangedEventArgs e)
    {
        _logger?.LogInformation("Shard topology changed: {Added} added, {Removed} removed",
            e.AddedShards.Count, e.RemovedShards.Count);
    }

    private IShardClient GetClient(int shardId)
    {
        lock (_clientLock)
        {
            if (_clients.TryGetValue(shardId, out var client))
                return client;

            throw new InvalidOperationException($"Shard {shardId} not found in cluster");
        }
    }

    private IShardClient GetClientForKey(string key)
    {
        var shardId = _router.GetShardId(key);
        return GetClient(shardId);
    }

    // --- Database operations ---
    // Broadcast to all shards

    public (bool Ok, string? Error) CreateDb(string db)
    {
        return CreateDbAsync(db).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> CreateDbAsync(string db)
    {
        var tasks = _clients.Values.Select(c => c.CreateDbAsync(db)).ToList();
        var results = await Task.WhenAll(tasks);

        var errors = results.Where(r => !r.Ok).Select(r => r.Error).ToList();
        if (errors.Count > 0)
        {
            _logger?.LogWarning("CreateDb '{Db}' failed on some shards: {Errors}", db, string.Join("; ", errors));
            // Считаем успехом, если хотя бы один шард успешно создал БД
            if (results.Any(r => r.Ok))
                return (true, null);
            return (false, string.Join("; ", errors));
        }

        return (true, null);
    }

    public (bool Ok, string? Error) DeleteDb(string db)
    {
        return DeleteDbAsync(db).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> DeleteDbAsync(string db)
    {
        var tasks = _clients.Values.Select(c => c.DeleteDbAsync(db)).ToList();
        var results = await Task.WhenAll(tasks);

        var errors = results.Where(r => !r.Ok).Select(r => r.Error).ToList();
        if (errors.Count > 0)
        {
            _logger?.LogWarning("DeleteDb '{Db}' failed on some shards: {Errors}", db, string.Join("; ", errors));
            if (results.Any(r => r.Ok))
                return (true, null);
            return (false, string.Join("; ", errors));
        }

        return (true, null);
    }

    // --- Document CRUD ---
    // Route to specific shard by _id

    public Document? Get(string db, string id, string? rev = null)
    {
        return GetAsync(db, id, rev).GetAwaiter().GetResult();
    }

    private async Task<Document?> GetAsync(string db, string id, string? rev = null)
    {
        var client = GetClientForKey(id);
        return await client.GetAsync(db, id, rev);
    }

    public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
    {
        return PutAsync(db, doc).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc)
    {
        if (string.IsNullOrWhiteSpace(doc.Id))
            return (false, null, "Missing _id");

        var client = GetClientForKey(doc.Id);
        return await client.PutAsync(db, doc);
    }

    public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
    {
        return PostAsync(db, doc).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc)
    {
        // Если _id не задан, генерируем его на координаторе для правильной маршрутизации
        if (string.IsNullOrWhiteSpace(doc.Id))
            doc.Id = Guid.NewGuid().ToString("N");

        var client = GetClientForKey(doc.Id);
        return await client.PostAsync(db, doc);
    }

    public (bool Ok, string? Error) Delete(string db, string id, string rev)
    {
        return DeleteAsync(db, id, rev).GetAwaiter().GetResult();
    }

    private async Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev)
    {
        var client = GetClientForKey(id);
        return await client.DeleteAsync(db, id, rev);
    }

    // --- Scatter-Gather queries ---
    // Query all shards and merge results

    public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true)
    {
        return AllDocsAsync(db, skip, limit, includeDeleted).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> AllDocsAsync(string db, int skip, int limit, bool includeDeleted)
    {
        // Запрашиваем с каждого шарда с учётом пагинации
        // Для точной пагинации нужно запросить skip+limit документов с каждого шарда
        var perShardLimit = skip + limit;

        var tasks = _clients.Values
            .Select(c => c.AllDocsAsync(db, 0, perShardLimit, includeDeleted))
            .ToList();

        var results = await Task.WhenAll(tasks);

        // Объединяем, сортируем по Id и применяем пагинацию
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
        // Возвращаем максимальный seq среди всех шардов
        var tasks = _clients.Values.Select(c => c.SeqAsync(db)).ToList();
        var results = await Task.WhenAll(tasks);
        return results.Max();
    }

    public StatsDto Stats(string db)
    {
        return StatsAsync(db).GetAwaiter().GetResult();
    }

    private async Task<StatsDto> StatsAsync(string db)
    {
        var tasks = _clients.Values.Select(c => c.StatsAsync(db)).ToList();
        var results = await Task.WhenAll(tasks);

        // Агрегируем статистику со всех шардов
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

    // --- Search (Scatter-Gather) ---

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

        var tasks = _clients.Values
            .Select(c => c.FindByFieldsAsync(db, equals, numericRange, 0, perShardLimit))
            .ToList();

        var results = await Task.WhenAll(tasks);

        return results
            .SelectMany(r => r)
            .OrderBy(d => d.Id, StringComparer.Ordinal)
            .Skip(skip)
            .Take(limit);
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

        var tasks = _clients.Values
            .Select(c => c.FindByTagsAsync(db, allOf, anyOf, noneOf, 0, perShardLimit))
            .ToList();

        var results = await Task.WhenAll(tasks);

        return results
            .SelectMany(r => r)
            .OrderBy(d => d.Id, StringComparer.Ordinal)
            .Skip(skip)
            .Take(limit);
    }

    public IEnumerable<Document> FullTextSearch(string db, string query, int skip = 0, int limit = 100)
    {
        return FullTextSearchAsync(db, query, skip, limit).GetAwaiter().GetResult();
    }

    private async Task<IEnumerable<Document>> FullTextSearchAsync(string db, string query, int skip, int limit)
    {
        var perShardLimit = skip + limit;

        var tasks = _clients.Values
            .Select(c => c.FullTextSearchAsync(db, query, 0, perShardLimit))
            .ToList();

        var results = await Task.WhenAll(tasks);

        return results
            .SelectMany(r => r)
            .OrderBy(d => d.Id, StringComparer.Ordinal)
            .Skip(skip)
            .Take(limit);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _discovery.ShardsChanged -= OnShardsChanged;

        foreach (var client in _clients.Values)
        {
            try { client.Dispose(); } catch { }
        }
        _clients.Clear();

        if (_discovery is IDisposable disposable)
            disposable.Dispose();
    }
}

