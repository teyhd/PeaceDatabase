using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage;

namespace PeaceDatabase.Controllers;

[ApiController]
[Route("v1/db/{db?}")]
public class DbController : ControllerBase
{
    private readonly IDocumentService _service;

    public DbController(IDocumentService service)
    {
        _service = service;
    }

    // -------------------
    // Управление базами
    // -------------------
    [HttpPut]
    public IActionResult CreateDb(string db)
    {
        var (ok, err) = _service.CreateDb(db);
        return ok ? Ok(new { ok = true }) : BadRequest(new { error = err });
    }

    [HttpDelete]
    public IActionResult DeleteDb(string db)
    {
        var (ok, err) = _service.DeleteDb(db);
        return ok ? Ok(new { ok = true }) : NotFound(new { error = err });
    }

    [HttpGet("{id}")]
    public IActionResult GetDoc(string db, string id)
    {
        var doc = _service.Get(db, id);
        return doc is null ? NotFound(new { error = "not_found" }) : Ok(doc);
    }

    [HttpPut("{id}")]
    public IActionResult PutDoc(string db, string id, [FromBody] Document doc)
    {
        doc.Id = id;
        var (ok, d, err) = _service.Put(db, doc);
        return ok ? Ok(d) : Conflict(new { error = err });
    }

    [HttpPost]
    public IActionResult PostDoc(string db, [FromBody] Document doc)
    {
        var (ok, d, err) = _service.Post(db, doc);
        return ok ? Created($"/v1/db/{db}/{d!.Id}", d) : BadRequest(new { error = err });
    }

    [HttpDelete("{id}")]
    public IActionResult DeleteDoc(string db, string id)
    {
        var (ok, err) = _service.Delete(db, id);
        return ok ? Ok(new { ok = true }) : NotFound(new { error = err });
    }

    [HttpGet("_all_docs")]
    public IActionResult AllDocs(string db, [FromQuery] int skip = 0, [FromQuery] int limit = 1000)
    {
        limit = Math.Min(limit, 1000);
        var docs = (_service as InMemoryDocumentService)?.AllDocs(db, skip, limit);
        return Ok(new { rows = docs });
    }

    [HttpGet("_changes")]
    public IActionResult Changes(string db)
    {
        if (_service is not InMemoryDocumentService mem) return NotFound();
        var seq = mem.Seq(db);
        return Ok(new { seq });
    }
}
