// File: Controllers/DbAndDocumentsController.cs
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Binary; // добавлено
using System.Diagnostics; // для бенчмарка
using Microsoft.AspNetCore.Http; // for IFormFile
using Microsoft.AspNetCore.Mvc.ApiExplorer; // for Tags attribute

namespace PeaceDatabase.WebApi.Controllers;

// Response models for better Swagger documentation
public class DatabaseResponse
{
    public bool Ok { get; set; }
    public string? Db { get; set; }
}

public class ErrorResponse
{
    public bool Ok { get; set; }
    public string? Error { get; set; }
    public string? Db { get; set; }
    public string? Id { get; set; }
    public string? Rev { get; set; }
}

public class AllDocsResponse
{
    public int Total { get; set; }
    public IEnumerable<Document>? Items { get; set; }
}

public class SequenceResponse
{
    public string? Db { get; set; }
    public int Seq { get; set; }
}

// ===============================
// Управление БД
// ===============================
[ApiController]
[Route("v1/db")]
[Produces("application/json")]
public class DbApiController : ControllerBase
{
    private readonly IDocumentService _svc;

    public DbApiController(IDocumentService svc)
    {
        _svc = svc ?? throw new ArgumentNullException(nameof(svc));
    }

    // HEAD /v1/db/{db}
    [HttpHead("{db}")]
    public IActionResult HeadDb([FromRoute] string db)
    {
        var res = _svc.CreateDb(db);
        if (!res.Ok) return Problem(res.Error ?? "db error", statusCode: 500);
        return NoContent();
    }

    // PUT /v1/db/{db}
    [HttpPut("{db}")]
    [ProducesResponseType(typeof(DatabaseResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public IActionResult CreateDb([FromRoute] string db)
    {
        var res = _svc.CreateDb(db);
        if (!res.Ok) return Problem(res.Error ?? "db create failed", statusCode: 500);
        return Ok(new DatabaseResponse { Ok = true, Db = db });
    }

    // DELETE /v1/db/{db}
    [HttpDelete("{db}")]
    [ProducesResponseType(typeof(DatabaseResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public IActionResult DeleteDb([FromRoute] string db)
    {
        var res = _svc.DeleteDb(db);
        if (!res.Ok)
        {
            var msg = res.Error ?? "db delete failed";
            if (msg.Contains("not found", StringComparison.OrdinalIgnoreCase))
                return NotFound(new ErrorResponse { Ok = false, Error = msg, Db = db });
            return Problem(msg, statusCode: 500);
        }
        return Ok(new DatabaseResponse { Ok = true, Db = db });
    }

    // GET /v1/db/{db}/_all_docs?skip=&limit=&includeDeleted=
    [HttpGet("{db}/_all_docs")]
    [ProducesResponseType(typeof(AllDocsResponse), StatusCodes.Status200OK)]
    public IActionResult AllDocs([FromRoute] string db, [FromQuery] int? skip, [FromQuery] int? limit, [FromQuery] bool? includeDeleted)
    {
        var docs = _svc.AllDocs(db, skip ?? 0, limit ?? 100, includeDeleted ?? true);
        return Ok(new AllDocsResponse { Total = docs?.Count() ?? 0, Items = docs });
    }

    // GET /v1/db/{db}/_seq
    [HttpGet("{db}/_seq")]
    [ProducesResponseType(typeof(SequenceResponse), StatusCodes.Status200OK)]
    public IActionResult Seq([FromRoute] string db)
    {
        var seq = _svc.Seq(db);
        return Ok(new SequenceResponse { Db = db, Seq = seq });
    }

    // ---- Поиск по полям ----
    public sealed class FindByFieldsRequest
    {
        // Переименовано, чтобы не скрывать object.Equals
        public Dictionary<string, string>? EqualsMap { get; set; }
        public string? NumericField { get; set; }
        public double? Min { get; set; }
        public double? Max { get; set; }
        public int? Skip { get; set; }
        public int? Limit { get; set; }
    }

    // POST /v1/db/{db}/_find/fields
    [HttpPost("{db}/_find/fields")]
    [ProducesResponseType(typeof(AllDocsResponse), StatusCodes.Status200OK)]
    public IActionResult FindByFields([FromRoute] string db, [FromBody, Required] FindByFieldsRequest req)
    {
        (string field, double? min, double? max)? range = null;
        if (!string.IsNullOrWhiteSpace(req.NumericField))
            range = (req.NumericField!, req.Min, req.Max);

        var docs = _svc.FindByFields(db, equals: req.EqualsMap, numericRange: range, skip: req.Skip ?? 0, limit: req.Limit ?? 100);
        return Ok(new AllDocsResponse { Total = docs?.Count() ?? 0, Items = docs });
    }

    // ---- Поиск по тегам ----
    public sealed class FindByTagsRequest
    {
        public IEnumerable<string>? AllOf { get; set; }
        public IEnumerable<string>? AnyOf { get; set; }
        public IEnumerable<string>? NoneOf { get; set; }
        public int? Skip { get; set; }
        public int? Limit { get; set; }
    }

    // POST /v1/db/{db}/_find/tags
    [HttpPost("{db}/_find/tags")]
    [ProducesResponseType(typeof(AllDocsResponse), StatusCodes.Status200OK)]
    public IActionResult FindByTags([FromRoute] string db, [FromBody, Required] FindByTagsRequest req)
    {
        var docs = _svc.FindByTags(db, allOf: req.AllOf, anyOf: req.AnyOf, noneOf: req.NoneOf, skip: req.Skip ?? 0, limit: req.Limit ?? 100);
        return Ok(new AllDocsResponse { Total = docs?.Count() ?? 0, Items = docs });
    }

    // ---- Полнотекст ----
    // GET /v1/db/{db}/_search?q=...&skip=&limit=
    [HttpGet("{db}/_search")]
    [ProducesResponseType(typeof(AllDocsResponse), StatusCodes.Status200OK)]
    public IActionResult FullText([FromRoute] string db, [FromQuery, Required] string q, [FromQuery] int? skip, [FromQuery] int? limit)
    {
        var docs = _svc.FullTextSearch(db, q, skip ?? 0, limit ?? 100);
        return Ok(new AllDocsResponse { Total = docs?.Count() ?? 0, Items = docs });
    }
}

// ===============================
// Документы
// ===============================
[ApiController]
[Route("v1/db/{db}/docs")]
[Produces("application/json")]
public class DocsApiController : ControllerBase
{
    private readonly IDocumentService _svc;
    private const string BinaryMime = "application/x-peacedb-doc";

    public DocsApiController(IDocumentService svc)
    {
        _svc = svc ?? throw new ArgumentNullException(nameof(svc));
    }

    // Model for multipart/form-data uploads (required for Swagger compatibility)
    public sealed class BinaryUploadForm
    {
        [Required]
        public IFormFile File { get; set; } = default!;
    }

    // GET /v1/db/{db}/docs/{id}?rev=
    [HttpGet("{id}")]
    [ProducesResponseType(typeof(Document), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    public IActionResult Get([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev = null)
    {
        var doc = _svc.Get(db, id, rev);
        if (doc == null)
            return NotFound(new ErrorResponse { Ok = false, Error = "not found", Db = db, Id = id, Rev = rev });
        return Ok(doc);
    }

    // GET /v1/db/{db}/docs/{id}/bin  (бинарный формат)
    [HttpGet("{id}/bin")]
    [Produces(BinaryMime)]
    public IActionResult GetBinary([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev = null)
    {
        var doc = _svc.Get(db, id, rev);
        if (doc == null)
            return NotFound(new ErrorResponse { Ok = false, Error = "not found", Db = db, Id = id, Rev = rev });
        var bytes = CustomBinaryDocumentCodec.Serialize(doc);
        return File(bytes, BinaryMime, fileDownloadName: id + ".pdoc");
    }

    // PUT /v1/db/{db}/docs/{id}
    [HttpPut("{id}")]
    [ProducesResponseType(typeof(Document), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public IActionResult Put([FromRoute] string db, [FromRoute] string id, [FromBody, Required] Document body)
    {
        var idProp = body.GetType().GetProperty("Id");
        var bodyId = idProp?.GetValue(body)?.ToString();
        if (bodyId != null && !string.Equals(bodyId, id, StringComparison.Ordinal))
            return BadRequest(new ErrorResponse { Ok = false, Error = "_id in body must equal route id", Db = db, Id = id });
        
        if (bodyId == null && idProp?.CanWrite == true)
            idProp.SetValue(body, id);

        var res = _svc.Put(db, body);
        if (!res.Ok)
        {
            var msg = res.Error ?? "put failed";
            if (msg.Contains("conflict", StringComparison.OrdinalIgnoreCase))
                return Conflict(new ErrorResponse { Ok = false, Error = msg, Db = db, Id = id });
            return Problem(msg, statusCode: 500);
        }

        if (res.Doc == null)
            return Problem("put returned null document", statusCode: 500);
        
        return Ok(res.Doc);
    }

    // PUT /v1/db/{db}/docs/{id}/bin  (принимает бинарный документ)
    [HttpPut("{id}/bin")]
    [Consumes(BinaryMime)]
    [Produces(BinaryMime)]
    public async Task<IActionResult> PutBinary([FromRoute] string db, [FromRoute] string id)
    {
        using var ms = new MemoryStream();
        await Request.Body.CopyToAsync(ms, HttpContext.RequestAborted);
        if (ms.Length == 0)
            return BadRequest(new { ok = false, error = "request body is empty. Send binary payload with Content-Type 'application/x-peacedb-doc'", db, id });
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        if (!string.Equals(doc.Id, id, StringComparison.Ordinal) && !string.IsNullOrEmpty(doc.Id))
            return BadRequest(new { ok = false, error = "_id in binary body must equal route id", routeId = id, bodyId = doc.Id });
        doc.Id = id; // нормализуем
        var res = _svc.Put(db, doc);
        if (!res.Ok)
        {
            var msg = res.Error ?? "put failed";
            if (msg.Contains("conflict", StringComparison.OrdinalIgnoreCase))
                return Conflict(new ErrorResponse { Ok = false, Error = msg, Db = db, Id = id });
            return Problem(msg, statusCode: 500);
        }
        var bytes = CustomBinaryDocumentCodec.Serialize(res.Doc!);
        return File(bytes, BinaryMime, fileDownloadName: id + ".pdoc");
    }

    // PUT /v1/db/{db}/docs/{id}/bin-upload (multipart/form-data for Swagger file upload)
    [HttpPut("{id}/bin-upload")]
    [Consumes("multipart/form-data")]
    [Produces(BinaryMime)]
    public async Task<IActionResult> PutBinaryUpload([FromRoute] string db, [FromRoute] string id, [FromForm] BinaryUploadForm form)
    {
        if (form.File == null || form.File.Length == 0)
            return BadRequest(new { ok = false, error = "file is required and cannot be empty" });
        using var ms = new MemoryStream();
        await form.File.CopyToAsync(ms, HttpContext.RequestAborted);
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        if (!string.Equals(doc.Id, id, StringComparison.Ordinal) && !string.IsNullOrEmpty(doc.Id))
            return BadRequest(new { ok = false, error = "_id in binary body must equal route id", routeId = id, bodyId = doc.Id });
        doc.Id = id;
        var res = _svc.Put(db, doc);
        if (!res.Ok)
        {
            var msg = res.Error ?? "put failed";
            if (msg.Contains("conflict", StringComparison.OrdinalIgnoreCase))
                return Conflict(new ErrorResponse { Ok = false, Error = msg, Db = db, Id = id });
            return Problem(msg, statusCode: 500);
        }
        var bytes = CustomBinaryDocumentCodec.Serialize(res.Doc!);
        return File(bytes, BinaryMime, fileDownloadName: id + ".pdoc");
    }

    // POST /v1/db/{db}/docs
    [HttpPost]
    public IActionResult Post([FromRoute] string db, [FromBody, Required] Document body)
    {
        var res = _svc.Post(db, body);
        if (!res.Ok) return Problem(res.Error ?? "post failed", statusCode: 500);
        if (res.Doc == null) return Problem("post returned null document", statusCode: 500);
        var idProp = res.Doc.GetType().GetProperty("Id");
        var idVal = idProp?.GetValue(res.Doc)?.ToString();
        var location = idVal != null ? $"/v1/db/{db}/docs/{idVal}" : $"/v1/db/{db}/docs";
        return Created(location, res.Doc);
    }

    // POST /v1/db/{db}/docs/bin  (бинарный формат)
    [HttpPost("bin")]
    [Consumes(BinaryMime)]
    [Produces(BinaryMime)]
    public async Task<IActionResult> PostBinary([FromRoute] string db)
    {
        using var ms = new MemoryStream();
        await Request.Body.CopyToAsync(ms, HttpContext.RequestAborted);
        if (ms.Length == 0)
            return BadRequest(new { ok = false, error = "request body is empty. Send binary payload with Content-Type 'application/x-peacedb-doc'", db });
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        // For create, ignore incoming _rev to satisfy service contract
        doc.Rev = null;
        var res = _svc.Post(db, doc);
        if (!res.Ok || res.Doc == null) return Problem(res.Error ?? "post failed", statusCode: 500);
        var bytes = CustomBinaryDocumentCodec.Serialize(res.Doc);
        var location = $"/v1/db/{db}/docs/{res.Doc.Id}/bin";
        return File(bytes, BinaryMime, fileDownloadName: res.Doc.Id + ".pdoc");
    }

    // POST /v1/db/{db}/docs/bin-upload (multipart/form-data for Swagger file upload)
    [HttpPost("bin-upload")]
    [Consumes("multipart/form-data")]
    [Produces(BinaryMime)]
    public async Task<IActionResult> PostBinaryUpload([FromRoute] string db, [FromForm] BinaryUploadForm form)
    {
        if (form.File == null || form.File.Length == 0)
            return BadRequest(new { ok = false, error = "file is required and cannot be empty" });
        using var ms = new MemoryStream();
        await form.File.CopyToAsync(ms, HttpContext.RequestAborted);
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        // For create, ignore incoming _rev to satisfy service contract
        doc.Rev = null;
        var res = _svc.Post(db, doc);
        if (!res.Ok || res.Doc == null) return Problem(res.Error ?? "post failed", statusCode: 500);
        var bytes = CustomBinaryDocumentCodec.Serialize(res.Doc);
        var location = $"/v1/db/{db}/docs/{res.Doc.Id}/bin";
        return File(bytes, BinaryMime, fileDownloadName: res.Doc.Id + ".pdoc");
    }

    // DELETE /v1/db/{db}/docs/{id}?rev=
    [HttpDelete("{id}")]
    public IActionResult Delete([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev)
    {
        if (string.IsNullOrWhiteSpace(rev))
            return BadRequest(new { ok = false, error = "rev query parameter is required", db, id });
        var res = _svc.Delete(db, id, rev!);
        if (!res.Ok)
        {
            var msg = res.Error ?? "delete failed";
            if (msg.Contains("not found", StringComparison.OrdinalIgnoreCase))
                return NotFound(new { ok = false, error = msg, db, id, rev });
            if (msg.Contains("conflict", StringComparison.OrdinalIgnoreCase))
                return Conflict(new { ok = false, error = msg, db, id, rev });
            return Problem(msg, statusCode: 500);
        }

        return Ok(new { ok = true, id, rev });
    }
}

// ===============================
// Бенчмарк бинарной vs JSON сериализации
// ===============================
[ApiController]
[Route("v1/bench")]
[Produces("application/json")]
public sealed class BenchApiController : ControllerBase
{
    private readonly IDocumentService _svc;
    public BenchApiController(IDocumentService svc) => _svc = svc;

    // GET /v1/bench/doc/{db}/{id}?iterations=1000
    [HttpGet("doc/{db}/{id}")]
    public IActionResult BenchmarkDoc([FromRoute] string db, [FromRoute] string id, [FromQuery] int? iterations)
    {
        var it = iterations.GetValueOrDefault(1000);
        if (it <= 0) it = 1;
        if (it > 100_000_000) it = 100_000_000;
        var doc = _svc.Get(db, id);
        if (doc == null) return NotFound(new { ok = false, error = "not found", db, id });

        // JSON serialize/deserialize
        var jsonSerSw = Stopwatch.StartNew();
        byte[]? lastJson = null;
        for (int i = 0; i < it; i++)
            lastJson = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(doc);
        jsonSerSw.Stop();
        var jsonDesSw = Stopwatch.StartNew();
        for (int i = 0; i < it; i++)
        {
            var d = System.Text.Json.JsonSerializer.Deserialize<Document>(lastJson!);
            if (d == null) return Problem("json deser failed", statusCode: 500);
        }
        jsonDesSw.Stop();

        // Binary serialize/deserialize
        var binSerSw = Stopwatch.StartNew();
        byte[]? lastBin = null;
        for (int i = 0; i < it; i++)
            lastBin = CustomBinaryDocumentCodec.Serialize(doc);
        binSerSw.Stop();
        var binDesSw = Stopwatch.StartNew();
        for (int i = 0; i < it; i++)
        {
            var d = CustomBinaryDocumentCodec.Deserialize(lastBin!);
            if (d == null) return Problem("bin deser failed", statusCode: 500);
        }
        binDesSw.Stop();

        return Ok(new
        {
            ok = true,
            db,
            id,
            iterations = it,
            sizes = new { json = lastJson!.Length, binary = lastBin!.Length },
            jsonMs = new { serialize = jsonSerSw.Elapsed.TotalMilliseconds, deserialize = jsonDesSw.Elapsed.TotalMilliseconds },
            binaryMs = new { serialize = binSerSw.Elapsed.TotalMilliseconds, deserialize = binDesSw.Elapsed.TotalMilliseconds },
            speedup = new
            {
                serialize = jsonSerSw.Elapsed.TotalMilliseconds / Math.Max(0.000001, binSerSw.Elapsed.TotalMilliseconds),
                deserialize = jsonDesSw.Elapsed.TotalMilliseconds / Math.Max(0.000001, binDesSw.Elapsed.TotalMilliseconds)
            }
        });
    }
}
