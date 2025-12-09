using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Client;

/// <summary>
/// Локальный клиент шарда для in-process режима.
/// Напрямую вызывает IDocumentService без сетевых запросов.
/// </summary>
public sealed class LocalShardClient : IShardClient
{
    private readonly IDocumentService _service;
    private bool _disposed;

    public ShardInfo ShardInfo { get; }

    public LocalShardClient(ShardInfo shardInfo, IDocumentService service)
    {
        ShardInfo = shardInfo ?? throw new ArgumentNullException(nameof(shardInfo));
        _service = service ?? throw new ArgumentNullException(nameof(service));
        ShardInfo.Status = ShardStatus.Healthy;
    }

    public Task<bool> HealthCheckAsync(CancellationToken ct = default)
    {
        ShardInfo.Status = ShardStatus.Healthy;
        ShardInfo.LastHealthCheck = DateTimeOffset.UtcNow;
        return Task.FromResult(true);
    }

    // --- Database operations ---

    public Task<(bool Ok, string? Error)> CreateDbAsync(string db, CancellationToken ct = default)
    {
        var result = _service.CreateDb(db);
        return Task.FromResult(result);
    }

    public Task<(bool Ok, string? Error)> DeleteDbAsync(string db, CancellationToken ct = default)
    {
        var result = _service.DeleteDb(db);
        return Task.FromResult(result);
    }

    // --- Document CRUD ---

    public Task<Document?> GetAsync(string db, string id, string? rev = null, CancellationToken ct = default)
    {
        var result = _service.Get(db, id, rev);
        return Task.FromResult(result);
    }

    public Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc, CancellationToken ct = default)
    {
        var result = _service.Put(db, doc);
        return Task.FromResult(result);
    }

    public Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc, CancellationToken ct = default)
    {
        var result = _service.Post(db, doc);
        return Task.FromResult(result);
    }

    public Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev, CancellationToken ct = default)
    {
        var result = _service.Delete(db, id, rev);
        return Task.FromResult(result);
    }

    // --- Queries ---

    public Task<IReadOnlyList<Document>> AllDocsAsync(string db, int skip = 0, int limit = 1000, bool includeDeleted = true, CancellationToken ct = default)
    {
        var docs = _service.AllDocs(db, skip, limit, includeDeleted).ToList();
        return Task.FromResult<IReadOnlyList<Document>>(docs);
    }

    public Task<int> SeqAsync(string db, CancellationToken ct = default)
    {
        var seq = _service.Seq(db);
        return Task.FromResult(seq);
    }

    public Task<StatsDto> StatsAsync(string db, CancellationToken ct = default)
    {
        var stats = _service.Stats(db);
        return Task.FromResult(stats);
    }

    // --- Search ---

    public Task<IReadOnlyList<Document>> FindByFieldsAsync(
        string db,
        IDictionary<string, string>? equals = null,
        (string field, double? min, double? max)? numericRange = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default)
    {
        var docs = _service.FindByFields(db, equals, numericRange, skip, limit).ToList();
        return Task.FromResult<IReadOnlyList<Document>>(docs);
    }

    public Task<IReadOnlyList<Document>> FindByTagsAsync(
        string db,
        IEnumerable<string>? allOf = null,
        IEnumerable<string>? anyOf = null,
        IEnumerable<string>? noneOf = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default)
    {
        var docs = _service.FindByTags(db, allOf, anyOf, noneOf, skip, limit).ToList();
        return Task.FromResult<IReadOnlyList<Document>>(docs);
    }

    public Task<IReadOnlyList<Document>> FullTextSearchAsync(string db, string query, int skip = 0, int limit = 100, CancellationToken ct = default)
    {
        var docs = _service.FullTextSearch(db, query, skip, limit).ToList();
        return Task.FromResult<IReadOnlyList<Document>>(docs);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // IDocumentService управляется извне
    }
}

