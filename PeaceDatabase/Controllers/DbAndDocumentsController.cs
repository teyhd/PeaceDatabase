// File: Controllers/DbAndDocumentsController.cs
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Binary; // добавлено
using System.Diagnostics; // для бенчмарка
using PeaceDatabase.WebApi.Exceptions;

namespace PeaceDatabase.WebApi.Controllers;

// ===============================
// Управление БД
// ===============================
[ApiController]
[Route("v1/db")]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status409Conflict)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
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
        ControllerErrorMapper.ThrowIfFailed(_svc.CreateDb(db), db);
        return NoContent();
    }

    // PUT /v1/db/{db}
    [HttpPut("{db}")]
    public IActionResult CreateDb([FromRoute] string db)
    {
        ControllerErrorMapper.ThrowIfFailed(_svc.CreateDb(db), db);
        return Ok(new { ok = true, db });
    }

    // DELETE /v1/db/{db}
    [HttpDelete("{db}")]
    public IActionResult DeleteDb([FromRoute] string db)
    {
        ControllerErrorMapper.ThrowIfFailed(_svc.DeleteDb(db), db);
        return Ok(new { ok = true, db });
    }

    // GET /v1/db/{db}/_all_docs?skip=&limit=&includeDeleted=
    [HttpGet("{db}/_all_docs")]
    public IActionResult AllDocs([FromRoute] string db, [FromQuery] int? skip, [FromQuery] int? limit, [FromQuery] bool? includeDeleted)
    {
        var docs = _svc.AllDocs(db, skip ?? 0, limit ?? 100, includeDeleted ?? true);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }

    // GET /v1/db/{db}/_seq
    [HttpGet("{db}/_seq")]
    public IActionResult Seq([FromRoute] string db)
    {
        var seq = _svc.Seq(db);
        return Ok(new { db, seq });
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
    public IActionResult FindByFields([FromRoute] string db, [FromBody, Required] FindByFieldsRequest req)
    {
        if (req is null)
        {
            throw new ArgumentException("Request body is required.", nameof(req));
        }

        (string field, double? min, double? max)? range = null;
        if (!string.IsNullOrWhiteSpace(req.NumericField))
            range = (req.NumericField!, req.Min, req.Max);

        var docs = _svc.FindByFields(db, equals: req.EqualsMap, numericRange: range, skip: req.Skip ?? 0, limit: req.Limit ?? 100);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
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
    public IActionResult FindByTags([FromRoute] string db, [FromBody, Required] FindByTagsRequest req)
    {
        var docs = _svc.FindByTags(db, allOf: req.AllOf, anyOf: req.AnyOf, noneOf: req.NoneOf, skip: req.Skip ?? 0, limit: req.Limit ?? 100);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }

    // ---- Полнотекст ----
    // GET /v1/db/{db}/_search?q=...&skip=&limit=
    [HttpGet("{db}/_search")]
    public IActionResult FullText([FromRoute] string db, [FromQuery, Required] string q, [FromQuery] int? skip, [FromQuery] int? limit)
    {
        var docs = _svc.FullTextSearch(db, q, skip ?? 0, limit ?? 100);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }
}

// ===============================
// Документы
// ===============================
[ApiController]
[Route("v1/db/{db}/docs")]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status409Conflict)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
public class DocsApiController : ControllerBase
{
    private readonly IDocumentService _svc;
    private const string BinaryMime = "application/x-peacedb-doc";

    public DocsApiController(IDocumentService svc)
    {
        _svc = svc ?? throw new ArgumentNullException(nameof(svc));
    }

    // GET /v1/db/{db}/docs/{id}?rev=
    [HttpGet("{id}")]
    public IActionResult Get([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev = null)
    {
        var doc = _svc.Get(db, id, rev);
        if (doc == null)
            throw new ResourceNotFoundException($"Document '{id}' was not found in '{db}'.");
        return Ok(doc);
    }

    // GET /v1/db/{db}/docs/{id}/bin  (бинарный формат)
    [HttpGet("{id}/bin")]
    [Produces(BinaryMime)]
    public IActionResult GetBinary([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev = null)
    {
        var doc = _svc.Get(db, id, rev);
        if (doc == null)
            throw new ResourceNotFoundException($"Document '{id}' was not found in '{db}'.");
        var bytes = CustomBinaryDocumentCodec.Serialize(doc);
        return File(bytes, BinaryMime, fileDownloadName: id + ".pdoc");
    }

    // PUT /v1/db/{db}/docs/{id}
    [HttpPut("{id}")]
    public IActionResult Put([FromRoute] string db, [FromRoute] string id, [FromBody, Required] Document body)
    {
        var idProp = body.GetType().GetProperty("Id");
        var bodyId = idProp?.GetValue(body)?.ToString();
        if (bodyId != null && !string.Equals(bodyId, id, StringComparison.Ordinal))
        {
            throw new DomainValidationException(
                "Route id and body _id must match.",
                new Dictionary<string, string[]>
                {
                    ["id"] = new[] { "Route id and body _id must match." },
                    ["_id"] = new[] { "Route id and body _id must match." }
                });
        }

        if (bodyId == null && idProp?.CanWrite == true)
            idProp.SetValue(body, id);

        var doc = ControllerErrorMapper.EnsureDocument(_svc.Put(db, body), db, id);
        return Ok(doc);
    }

    // PUT /v1/db/{db}/docs/{id}/bin  (принимает бинарный документ)
    [HttpPut("{id}/bin")]
    [Consumes(BinaryMime)]
    [Produces(BinaryMime)]
    public IActionResult PutBinary([FromRoute] string db, [FromRoute] string id)
    {
        using var ms = new MemoryStream();
        Request.Body.CopyTo(ms);
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        if (!string.Equals(doc.Id, id, StringComparison.Ordinal) && !string.IsNullOrEmpty(doc.Id))
        {
            throw new DomainValidationException(
                "Route id and body _id must match.",
                new Dictionary<string, string[]>
                {
                    ["id"] = new[] { "Route id and body _id must match." },
                    ["_id"] = new[] { "Route id and body _id must match." }
                });
        }
        doc.Id = id; // нормализуем
        var updated = ControllerErrorMapper.EnsureDocument(_svc.Put(db, doc), db, id);
        var bytes = CustomBinaryDocumentCodec.Serialize(updated);
        return File(bytes, BinaryMime, fileDownloadName: id + ".pdoc");
    }

    // POST /v1/db/{db}/docs
    [HttpPost]
    public IActionResult Post([FromRoute] string db, [FromBody, Required] Document body)
    {
        var created = ControllerErrorMapper.EnsureDocument(_svc.Post(db, body), db);
        var idProp = created.GetType().GetProperty("Id");
        var idVal = idProp?.GetValue(created)?.ToString();
        var location = idVal != null ? $"/v1/db/{db}/docs/{idVal}" : $"/v1/db/{db}/docs";
        return Created(location, created);
    }

    // POST /v1/db/{db}/docs/bin  (бинарный формат)
    [HttpPost("bin")]
    [Consumes(BinaryMime)]
    [Produces(BinaryMime)]
    public IActionResult PostBinary([FromRoute] string db)
    {
        using var ms = new MemoryStream();
        Request.Body.CopyTo(ms);
        var doc = CustomBinaryDocumentCodec.Deserialize(ms.ToArray());
        var created = ControllerErrorMapper.EnsureDocument(_svc.Post(db, doc), db);
        var bytes = CustomBinaryDocumentCodec.Serialize(created);
        var location = $"/v1/db/{db}/docs/{created.Id}/bin";
        return File(bytes, BinaryMime, fileDownloadName: created.Id + ".pdoc");
    }

    // DELETE /v1/db/{db}/docs/{id}?rev=
    [HttpDelete("{id}")]
    public IActionResult Delete([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev)
    {
        if (string.IsNullOrWhiteSpace(rev))
        {
            throw new DomainValidationException(
                "The rev query parameter is required.",
                new Dictionary<string, string[]>
                {
                    ["rev"] = new[] { "The rev query parameter is required." }
                });
        }
        ControllerErrorMapper.ThrowIfFailed(_svc.Delete(db, id, rev!), db, id);
        return Ok(new { ok = true, id, rev });
    }
}

// ===============================
// Бенчмарк бинарной vs JSON сериализации
// ===============================
[ApiController]
[Route("v1/bench")]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
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
        if (doc == null) throw new ResourceNotFoundException($"Document '{id}' was not found in '{db}'.");

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
            if (d == null) throw new InvalidOperationException("JSON deserialization returned null.");
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
            if (d == null) throw new InvalidOperationException("Binary deserialization returned null.");
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

