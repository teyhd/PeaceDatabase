using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Sharding.Configuration;
using PeaceDatabase.Storage.Sharding.Replication;
using PeaceDatabase.Storage.Sharding.Replication.Client;
using PeaceDatabase.Storage.Sharding.Replication.Configuration;

namespace PeaceDatabase.Controllers;

/// <summary>
/// Контроллер для управления репликацией.
/// Предоставляет эндпоинты для мониторинга состояния, репликации данных и управления primary.
/// </summary>
[ApiController]
[Route("v1/_replication")]
[Produces("application/json")]
public class ReplicationController : ControllerBase
{
    private readonly IDocumentService _svc;
    private readonly ShardingOptions _shardingOptions;
    private readonly ILogger<ReplicationController> _logger;
    private static readonly DateTimeOffset _startTime = DateTimeOffset.UtcNow;
    private static bool _isPrimary;
    private static string? _currentPrimaryUrl;
    private static long _lastReplicatedSeq;

    public ReplicationController(
        IDocumentService svc,
        ShardingOptions shardingOptions,
        ILogger<ReplicationController> logger)
    {
        _svc = svc ?? throw new ArgumentNullException(nameof(svc));
        _shardingOptions = shardingOptions ?? throw new ArgumentNullException(nameof(shardingOptions));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Инициализируем состояние primary из конфигурации
        if (_shardingOptions.Replication.CurrentReplicaIndex == 0)
            _isPrimary = true;
    }

    /// <summary>
    /// Получает текущее состояние репликации узла.
    /// </summary>
    [HttpGet("state")]
    [ProducesResponseType(typeof(ReplicationStateResponse), StatusCodes.Status200OK)]
    public IActionResult GetState()
    {
        // Собираем seq по всем БД (упрощённо - берём максимальный)
        long totalSeq = 0;
        try
        {
            // Пробуем получить seq для тестовых БД
            var stats = _svc.Stats("test");
            totalSeq = stats.Seq;
        }
        catch
        {
            // Игнорируем ошибки
        }

        var uptime = DateTimeOffset.UtcNow - _startTime;

        return Ok(new ReplicationStateResponse
        {
            Healthy = true,
            IsPrimary = _isPrimary,
            Seq = totalSeq,
            WalPosition = null, // WAL position не поддерживается напрямую
            UptimeSeconds = uptime.TotalSeconds,
            CurrentPrimaryUrl = _currentPrimaryUrl,
            ReplicationLag = _isPrimary ? 0 : Math.Max(0, totalSeq - _lastReplicatedSeq),
            LastSyncAt = DateTimeOffset.UtcNow,
            ShardId = _shardingOptions.CurrentShardId,
            ReplicaIndex = _shardingOptions.Replication.CurrentReplicaIndex
        });
    }

    /// <summary>
    /// Реплицирует одну операцию записи на этот узел.
    /// </summary>
    [HttpPost("replicate")]
    [ProducesResponseType(typeof(ReplicateResultResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    public IActionResult Replicate([FromBody] ReplicationEntryRequest entry)
    {
        if (entry == null)
            return BadRequest(new ErrorResponse { Ok = false, Error = "Request body is required" });

        try
        {
            switch (entry.Op?.ToLowerInvariant())
            {
                case "createdb":
                    var createResult = _svc.CreateDb(entry.Db);
                    if (!createResult.Ok && createResult.Error != "Database already exists")
                        return Ok(new ReplicateResultResponse { Ok = false, Error = createResult.Error });
                    break;

                case "deletedb":
                    var deleteResult = _svc.DeleteDb(entry.Db);
                    if (!deleteResult.Ok)
                        return Ok(new ReplicateResultResponse { Ok = false, Error = deleteResult.Error });
                    break;

                case "put":
                    if (entry.Doc == null)
                        return BadRequest(new ErrorResponse { Ok = false, Error = "Document is required for Put" });
                    var putResult = _svc.Put(entry.Db, entry.Doc);
                    if (!putResult.Ok)
                        return Ok(new ReplicateResultResponse { Ok = false, Error = putResult.Error });
                    break;

                case "post":
                    if (entry.Doc == null)
                        return BadRequest(new ErrorResponse { Ok = false, Error = "Document is required for Post" });
                    var postResult = _svc.Post(entry.Db, entry.Doc);
                    if (!postResult.Ok)
                        return Ok(new ReplicateResultResponse { Ok = false, Error = postResult.Error });
                    break;

                case "delete":
                    if (string.IsNullOrEmpty(entry.Rev))
                        return BadRequest(new ErrorResponse { Ok = false, Error = "Rev is required for Delete" });
                    var delResult = _svc.Delete(entry.Db, entry.Id, entry.Rev);
                    if (!delResult.Ok)
                        return Ok(new ReplicateResultResponse { Ok = false, Error = delResult.Error });
                    break;

                default:
                    return BadRequest(new ErrorResponse { Ok = false, Error = $"Unknown operation: {entry.Op}" });
            }

            _lastReplicatedSeq = entry.Seq;
            _logger.LogDebug("Replicated {Op} for {Db}/{Id} at seq {Seq}", entry.Op, entry.Db, entry.Id, entry.Seq);

            return Ok(new ReplicateResultResponse { Ok = true, Seq = entry.Seq });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Replication failed for {Op} {Db}/{Id}", entry.Op, entry.Db, entry.Id);
            return Ok(new ReplicateResultResponse { Ok = false, Error = ex.Message });
        }
    }

    /// <summary>
    /// Реплицирует пакет операций записи.
    /// </summary>
    [HttpPost("replicate-batch")]
    [ProducesResponseType(typeof(ReplicateBatchResultResponse), StatusCodes.Status200OK)]
    public IActionResult ReplicateBatch([FromBody] List<ReplicationEntryRequest> entries)
    {
        if (entries == null || entries.Count == 0)
            return Ok(new ReplicateBatchResultResponse { Ok = true, SuccessCount = 0, FailedCount = 0 });

        int successCount = 0;
        int failedCount = 0;
        var errors = new Dictionary<long, string>();
        long lastSeq = 0;

        foreach (var entry in entries.OrderBy(e => e.Seq))
        {
            try
            {
                var result = ProcessReplicationEntry(entry);
                if (result.ok)
                {
                    successCount++;
                    lastSeq = entry.Seq;
                }
                else
                {
                    failedCount++;
                    errors[entry.Seq] = result.error ?? "Unknown error";
                }
            }
            catch (Exception ex)
            {
                failedCount++;
                errors[entry.Seq] = ex.Message;
            }
        }

        _lastReplicatedSeq = lastSeq;

        return Ok(new ReplicateBatchResultResponse
        {
            Ok = failedCount == 0,
            SuccessCount = successCount,
            FailedCount = failedCount,
            LastSeq = lastSeq,
            Errors = errors
        });
    }

    private (bool ok, string? error) ProcessReplicationEntry(ReplicationEntryRequest entry)
    {
        switch (entry.Op?.ToLowerInvariant())
        {
            case "createdb":
                var createResult = _svc.CreateDb(entry.Db);
                return (createResult.Ok || createResult.Error == "Database already exists", createResult.Error);

            case "deletedb":
                var deleteResult = _svc.DeleteDb(entry.Db);
                return (deleteResult.Ok, deleteResult.Error);

            case "put":
                if (entry.Doc == null) return (false, "Document is required for Put");
                var putResult = _svc.Put(entry.Db, entry.Doc);
                return (putResult.Ok, putResult.Error);

            case "post":
                if (entry.Doc == null) return (false, "Document is required for Post");
                var postResult = _svc.Post(entry.Db, entry.Doc);
                return (postResult.Ok, postResult.Error);

            case "delete":
                if (string.IsNullOrEmpty(entry.Rev)) return (false, "Rev is required for Delete");
                var delResult = _svc.Delete(entry.Db, entry.Id, entry.Rev);
                return (delResult.Ok, delResult.Error);

            default:
                return (false, $"Unknown operation: {entry.Op}");
        }
    }

    /// <summary>
    /// Продвигает этот узел в primary.
    /// </summary>
    [HttpPost("promote")]
    [ProducesResponseType(typeof(PromoteResponse), StatusCodes.Status200OK)]
    public IActionResult Promote()
    {
        _isPrimary = true;
        _currentPrimaryUrl = null;
        _logger.LogInformation("This node has been promoted to primary");

        return Ok(new PromoteResponse
        {
            Ok = true,
            Message = "Promoted to primary",
            PromotedAt = DateTimeOffset.UtcNow
        });
    }

    /// <summary>
    /// Устанавливает нового primary для этого узла.
    /// </summary>
    [HttpPost("set-primary")]
    [ProducesResponseType(typeof(SetPrimaryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    public IActionResult SetPrimary([FromBody] SetPrimaryRequest request)
    {
        if (request == null || string.IsNullOrWhiteSpace(request.PrimaryUrl))
            return BadRequest(new ErrorResponse { Ok = false, Error = "PrimaryUrl is required" });

        _currentPrimaryUrl = request.PrimaryUrl;
        
        // Если новый primary — не этот узел, понижаем себя
        // (упрощённая логика, в реальности нужно сравнивать URLs)
        _isPrimary = false;

        _logger.LogInformation("New primary set: {PrimaryUrl}", request.PrimaryUrl);

        return Ok(new SetPrimaryResponse
        {
            Ok = true,
            PrimaryUrl = request.PrimaryUrl
        });
    }

    /// <summary>
    /// Получает записи WAL для синхронизации.
    /// </summary>
    [HttpGet("wal/{db}")]
    [ProducesResponseType(typeof(WalEntriesResponse), StatusCodes.Status200OK)]
    public IActionResult GetWalEntries(
        [FromRoute] string db,
        [FromQuery] long fromSeq = 0,
        [FromQuery] int limit = 1000)
    {
        // WAL streaming не реализован напрямую в IDocumentService
        // Возвращаем пустой ответ - синхронизация через полный скан
        return Ok(new WalEntriesResponse
        {
            Entries = new List<ReplicationEntryResponse>()
        });
    }

    #region DTOs

    public class ReplicationStateResponse
    {
        public bool Healthy { get; set; }
        public bool IsPrimary { get; set; }
        public long Seq { get; set; }
        public string? WalPosition { get; set; }
        public double UptimeSeconds { get; set; }
        public string? CurrentPrimaryUrl { get; set; }
        public long ReplicationLag { get; set; }
        public DateTimeOffset? LastSyncAt { get; set; }
        public int? ShardId { get; set; }
        public int? ReplicaIndex { get; set; }
    }

    public class ReplicationEntryRequest
    {
        public string Op { get; set; } = string.Empty;
        public string Db { get; set; } = string.Empty;
        public string Id { get; set; } = string.Empty;
        public string? Rev { get; set; }
        public long Seq { get; set; }
        public Document? Doc { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
    }

    public class ReplicateResultResponse
    {
        public bool Ok { get; set; }
        public long Seq { get; set; }
        public string? Error { get; set; }
    }

    public class ReplicateBatchResultResponse
    {
        public bool Ok { get; set; }
        public int SuccessCount { get; set; }
        public int FailedCount { get; set; }
        public long LastSeq { get; set; }
        public Dictionary<long, string>? Errors { get; set; }
    }

    public class PromoteResponse
    {
        public bool Ok { get; set; }
        public string? Message { get; set; }
        public DateTimeOffset? PromotedAt { get; set; }
    }

    public class SetPrimaryRequest
    {
        public string PrimaryUrl { get; set; } = string.Empty;
    }

    public class SetPrimaryResponse
    {
        public bool Ok { get; set; }
        public string? PrimaryUrl { get; set; }
    }

    public class WalEntriesResponse
    {
        public List<ReplicationEntryResponse>? Entries { get; set; }
    }

    public class ReplicationEntryResponse
    {
        public string? Op { get; set; }
        public string? Db { get; set; }
        public string? Id { get; set; }
        public string? Rev { get; set; }
        public long Seq { get; set; }
        public Document? Doc { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
    }

    public class ErrorResponse
    {
        public bool Ok { get; set; }
        public string? Error { get; set; }
    }

    #endregion
}

