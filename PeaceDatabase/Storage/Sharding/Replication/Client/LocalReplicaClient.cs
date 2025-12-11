using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Client;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication.Client;

/// <summary>
/// Клиент для локальной реплики (in-process).
/// Используется в режиме Local для тестирования и разработки.
/// </summary>
public sealed class LocalReplicaClient : IReplicaClient
{
    private readonly IDocumentService _service;
    private readonly DateTimeOffset _startTime;
    private bool _isPrimary;
    private string? _currentPrimaryUrl;
    private bool _disposed;

    public ReplicaInfo ReplicaInfo { get; }

    public ShardInfo ShardInfo => new()
    {
        Id = ReplicaInfo.ShardId,
        BaseUrl = ReplicaInfo.BaseUrl,
        IsLocal = true,
        Status = ShardStatus.Healthy
    };

    public LocalReplicaClient(ReplicaInfo replicaInfo, IDocumentService service, bool isPrimary = false)
    {
        ReplicaInfo = replicaInfo ?? throw new ArgumentNullException(nameof(replicaInfo));
        _service = service ?? throw new ArgumentNullException(nameof(service));
        _isPrimary = isPrimary;
        _startTime = DateTimeOffset.UtcNow;
    }

    #region IShardClient Implementation

    public Task<bool> HealthCheckAsync(CancellationToken ct = default)
    {
        return Task.FromResult(true);
    }

    public Task<(bool Ok, string? Error)> CreateDbAsync(string db, CancellationToken ct = default)
    {
        return Task.FromResult(_service.CreateDb(db));
    }

    public Task<(bool Ok, string? Error)> DeleteDbAsync(string db, CancellationToken ct = default)
    {
        return Task.FromResult(_service.DeleteDb(db));
    }

    public Task<Document?> GetAsync(string db, string id, string? rev = null, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Get(db, id, rev));
    }

    public Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Put(db, doc));
    }

    public Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Post(db, doc));
    }

    public Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Delete(db, id, rev));
    }

    public Task<IReadOnlyList<Document>> AllDocsAsync(string db, int skip = 0, int limit = 1000, bool includeDeleted = true, CancellationToken ct = default)
    {
        var docs = _service.AllDocs(db, skip, limit, includeDeleted).ToList();
        return Task.FromResult<IReadOnlyList<Document>>(docs);
    }

    public Task<int> SeqAsync(string db, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Seq(db));
    }

    public Task<StatsDto> StatsAsync(string db, CancellationToken ct = default)
    {
        return Task.FromResult(_service.Stats(db));
    }

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

    #endregion

    #region IReplicaClient Implementation

    public Task<ReplicationState> GetReplicationStateAsync(CancellationToken ct = default)
    {
        // Получаем общий seq по всем БД
        // Для простоты берём 0 если нет БД
        long totalSeq = 0;
        try
        {
            // Пытаемся получить seq для тестовой БД
            totalSeq = _service.Seq("_default") + _service.Seq("test");
        }
        catch
        {
            // Игнорируем ошибки
        }

        return Task.FromResult(new ReplicationState
        {
            IsHealthy = true,
            IsPrimary = _isPrimary,
            Seq = totalSeq,
            WalPosition = null,
            Uptime = DateTimeOffset.UtcNow - _startTime,
            CurrentPrimaryUrl = _currentPrimaryUrl,
            ReplicationLag = 0,
            LastSyncAt = DateTimeOffset.UtcNow
        });
    }

    public Task<ReplicateResult> ReplicateAsync(ReplicationEntry entry, CancellationToken ct = default)
    {
        try
        {
            switch (entry.Op)
            {
                case ReplicationOp.CreateDb:
                    var createResult = _service.CreateDb(entry.Db);
                    if (!createResult.Ok && createResult.Error != "Database already exists")
                        return Task.FromResult(ReplicateResult.Failure(createResult.Error ?? "Failed to create db"));
                    break;

                case ReplicationOp.DeleteDb:
                    var deleteResult = _service.DeleteDb(entry.Db);
                    if (!deleteResult.Ok)
                        return Task.FromResult(ReplicateResult.Failure(deleteResult.Error ?? "Failed to delete db"));
                    break;

                case ReplicationOp.Put:
                    if (entry.Doc == null)
                        return Task.FromResult(ReplicateResult.Failure("Document is required for Put"));
                    
                    var putResult = _service.Put(entry.Db, entry.Doc);
                    if (!putResult.Ok)
                        return Task.FromResult(ReplicateResult.Failure(putResult.Error ?? "Failed to put document"));
                    break;

                case ReplicationOp.Post:
                    if (entry.Doc == null)
                        return Task.FromResult(ReplicateResult.Failure("Document is required for Post"));
                    
                    var postResult = _service.Post(entry.Db, entry.Doc);
                    if (!postResult.Ok)
                        return Task.FromResult(ReplicateResult.Failure(postResult.Error ?? "Failed to post document"));
                    break;

                case ReplicationOp.Delete:
                    if (string.IsNullOrEmpty(entry.Rev))
                        return Task.FromResult(ReplicateResult.Failure("Rev is required for Delete"));
                    
                    var delResult = _service.Delete(entry.Db, entry.Id, entry.Rev);
                    if (!delResult.Ok)
                        return Task.FromResult(ReplicateResult.Failure(delResult.Error ?? "Failed to delete document"));
                    break;
            }

            return Task.FromResult(ReplicateResult.Success(entry.Seq));
        }
        catch (Exception ex)
        {
            return Task.FromResult(ReplicateResult.Failure(ex.Message));
        }
    }

    public async Task<ReplicateBatchResult> ReplicateBatchAsync(IEnumerable<ReplicationEntry> entries, CancellationToken ct = default)
    {
        var result = new ReplicateBatchResult();
        var errors = new Dictionary<long, string>();
        int successCount = 0;
        int failedCount = 0;
        long lastSeq = 0;

        foreach (var entry in entries)
        {
            var r = await ReplicateAsync(entry, ct);
            if (r.Ok)
            {
                successCount++;
                lastSeq = entry.Seq;
            }
            else
            {
                failedCount++;
                errors[entry.Seq] = r.Error ?? "Unknown error";
            }
        }

        return new ReplicateBatchResult
        {
            Ok = failedCount == 0,
            SuccessCount = successCount,
            FailedCount = failedCount,
            LastSeq = lastSeq,
            Errors = errors
        };
    }

    public Task PromoteAsync(CancellationToken ct = default)
    {
        _isPrimary = true;
        ReplicaInfo.IsPrimary = true;
        ReplicaInfo.PromotedAt = DateTimeOffset.UtcNow;
        return Task.CompletedTask;
    }

    public Task SetPrimaryAsync(string newPrimaryUrl, CancellationToken ct = default)
    {
        _currentPrimaryUrl = newPrimaryUrl;
        if (_isPrimary && newPrimaryUrl != ReplicaInfo.BaseUrl)
        {
            _isPrimary = false;
            ReplicaInfo.IsPrimary = false;
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<ReplicationEntry>> GetWalEntriesAsync(
        string db,
        long fromSeq,
        int limit = 1000,
        CancellationToken ct = default)
    {
        // Для локального режима WAL не поддерживается напрямую
        // Возвращаем пустой список - синхронизация будет через полный скан
        return Task.FromResult<IReadOnlyList<ReplicationEntry>>(Array.Empty<ReplicationEntry>());
    }

    #endregion

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // Не диспозим _service - он управляется извне
    }
}

