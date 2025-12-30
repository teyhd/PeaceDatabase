// File: Controllers/DbAndDocumentsController.cs
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
/*
namespace PeaceDatabase.WebApi.Controllers;

// -----------------------------
// Управление БД
// -----------------------------
[ApiController]
[Route("v1")]
public class DbController : ControllerBase
{
    private readonly IDocumentService _svc;
    public DbController(IDocumentService svc) => _svc = svc;

    // HEAD /v1/{db} — проверка/создание (идемпотентно)
    [HttpHead("{db}")]
    public IActionResult HeadDb([FromRoute] string db)
    {
        var res = _svc.CreateDb(db);
        if (!res.Ok) return Problem(res.Error ?? "db error", statusCode: 500);
        return NoContent();
    }

    // PUT /v1/{db} — создать БД (идемпотентно)
    [HttpPut("{db}")]
    public IActionResult CreateDb([FromRoute] string db)
    {
        var res = _svc.CreateDb(db);
        if (!res.Ok) return Problem(res.Error ?? "db create failed", statusCode: 500);
        return Ok(new { ok = true, db });
    }

    // DELETE /v1/{db} — удалить БД
    [HttpDelete("{db}")]
    public IActionResult DeleteDb([FromRoute] string db)
    {
        var res = _svc.DeleteDb(db);
        if (!res.Ok)
        {
            var msg = res.Error ?? "db delete failed";
            if (msg.Contains("not found", StringComparison.OrdinalIgnoreCase))
                return NotFound(new { ok = false, error = msg, db });
            return Problem(msg, statusCode: 500);
        }
        return Ok(new { ok = true, db });
    }

    // GET /v1/{db}/_all_docs?skip=&limit=&includeDeleted=
    [HttpGet("{db}/_all_docs")]
    public IActionResult AllDocs(
        [FromRoute] string db,
        [FromQuery] int? skip,
        [FromQuery] int? limit,
        [FromQuery] bool? includeDeleted)
    {
        var docs = _svc.AllDocs(db, skip ?? 0, limit ?? 100, includeDeleted ?? true);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }

    // GET /v1/{db}/_seq — текущая последовательность изменений
    [HttpGet("{db}/_seq")]
    public IActionResult Seq([FromRoute] string db)
    {
        var seq = _svc.Seq(db);
        return Ok(new { db, seq });
    }

    // -------- Поиск по полям (равенства + числовой диапазон) --------
    // POST /v1/{db}/_find/fields
    public sealed class FindByFieldsRequest
    {
        public Dictionary<string, string>? EqualsMap { get; set; }
        public string? NumericField { get; set; }
        public double? Min { get; set; }
        public double? Max { get; set; }
        public int? Skip { get; set; }
        public int? Limit { get; set; }
    }

    [HttpPost("{db}/_find/fields")]
    public IActionResult FindByFields([FromRoute] string db, [FromBody, Required] FindByFieldsRequest req)
    {
        (string field, double? min, double? max)? range = null;
        if (!string.IsNullOrWhiteSpace(req.NumericField))
            range = (req.NumericField!, req.Min, req.Max);

        var docs = _svc.FindByFields(
            db,
            equals: req.EqualsMap,
            numericRange: range,
            skip: req.Skip ?? 0,
            limit: req.Limit ?? 100);

        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }

    // -------- Поиск по тегам (allOf / anyOf / noneOf) --------
    // POST /v1/{db}/_find/tags
    public sealed class FindByTagsRequest
    {
        public IEnumerable<string>? AllOf { get; set; }
        public IEnumerable<string>? AnyOf { get; set; }
        public IEnumerable<string>? NoneOf { get; set; }
        public int? Skip { get; set; }
        public int? Limit { get; set; }
    }

    [HttpPost("{db}/_find/tags")]
    public IActionResult FindByTags([FromRoute] string db, [FromBody, Required] FindByTagsRequest req)
    {
        var docs = _svc.FindByTags(
            db,
            allOf: req.AllOf,
            anyOf: req.AnyOf,
            noneOf: req.NoneOf,
            skip: req.Skip ?? 0,
            limit: req.Limit ?? 100);

        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }

    // -------- Полнотекстовый поиск --------
    // GET /v1/{db}/_search?q=...&skip=&limit=
    [HttpGet("{db}/_search")]
    public IActionResult FullText(
        [FromRoute] string db,
        [FromQuery, Required] string q,
        [FromQuery] int? skip,
        [FromQuery] int? limit)
    {
        var docs = _svc.FullTextSearch(db, q, skip ?? 0, limit ?? 100);
        return Ok(new { total = docs?.Count() ?? 0, items = docs });
    }
}

// -----------------------------
// Документы
// -----------------------------
[ApiController]
[Route("v1/{db}")]
public class DocumentsController : ControllerBase
{
    private readonly IDocumentService _svc;
    public DocumentsController(IDocumentService svc) => _svc = svc;

    // GET /v1/{db}/{id}?rev=
    [HttpGet("{id}")]
    public IActionResult Get([FromRoute] string db, [FromRoute] string id, [FromQuery] string? rev = null)
    {
        var doc = _svc.Get(db, id, rev);
        if (doc == null)
            return NotFound(new { ok = false, error = "not found", db, id, rev });
        return Ok(doc);
    }

    // PUT /v1/{db}/{id} — upsert по _id
    // Принимаем целиком Document, чтобы не гадать про его поля.
    [HttpPut("{id}")]
    public IActionResult Put([FromRoute] string db, [FromRoute] string id, [FromBody, Required] Document body)
    {
        // Если у модели есть Id — проверим совпадение; если нет — не валидируем.
        var idProp = body.GetType().GetProperty("Id");
        var currentId = idProp?.GetValue(body)?.ToString();

        // Проверяем только если currentId не пустой (пустая строка = не указан)
        if (!string.IsNullOrEmpty(currentId) && !string.Equals(currentId, id, StringComparison.Ordinal))
            return BadRequest(new { ok = false, error = "_id in body must equal route id", routeId = id, bodyId = currentId });

        // Если Id в теле отсутствует или пустой — попробуем проставить его (если свойство есть и set доступен)
        if (string.IsNullOrEmpty(currentId) && idProp?.CanWrite == true)
            idProp.SetValue(body, id);

        var res = _svc.Put(db, body);
        if (!res.Ok)
        {
            var msg = res.Error ?? "put failed";
            if (msg.Contains("conflict", StringComparison.OrdinalIgnoreCase))
                return Conflict(new { ok = false, error = msg, db, id });
            return Problem(msg, statusCode: 500);
        }

        if (res.Doc == null)
            return Problem("put returned null document", statusCode: 500);

        return Ok(res.Doc);
    }

    // POST /v1/{db} — создать документ (auto-id на стороне сервиса)
    [HttpPost]
    public IActionResult Post([FromRoute] string db, [FromBody, Required] Document body)
    {
        var res = _svc.Post(db, body);
        if (!res.Ok)
            return Problem(res.Error ?? "post failed", statusCode: 500);

        if (res.Doc == null)
            return Problem("post returned null document", statusCode: 500);

        var idProp = res.Doc.GetType().GetProperty("Id");
        var idVal = idProp?.GetValue(res.Doc)?.ToString();
        var location = idVal != null ? $"/v1/{db}/{idVal}" : $"/v1/{db}";
        return Created(location, res.Doc);
    }

    // DELETE /v1/{db}/{id}?rev= — rev обязателен (интерфейс принимает non-null)
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
*/