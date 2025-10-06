// File: Controllers/TestApiController.cs
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.WebApi.Exceptions;

namespace PeaceDatabase.WebApi.Controllers;

/// <summary>
/// Тестовые ручки для проверки работы API и режима хранения.
/// Позволяют проверить доступность сервиса, работу с БД и сериализацию JSON.
/// </summary>
[ApiController]
[Route("v1/test")]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status400BadRequest)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status404NotFound)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status409Conflict)]
[ProducesResponseType(typeof(ProblemDetails), StatusCodes.Status500InternalServerError)]
public sealed class TestApiController : ControllerBase
{
    private readonly IConfiguration _cfg;
    private readonly IDocumentService _svc;

    public TestApiController(IConfiguration cfg, IDocumentService svc)
    {
        _cfg = cfg ?? throw new ArgumentNullException(nameof(cfg));
        _svc = svc ?? throw new ArgumentNullException(nameof(svc));
    }

    /// <summary>
    /// Проверка доступности API (ping).
    /// </summary>
    /// <remarks>
    /// Возвращает краткую информацию:
    /// - режим работы хранилища (InMemory или File);
    /// - путь к каталогу данных (если используется файловый режим);
    /// - текущее время UTC.
    ///
    /// Пример:
    /// GET /v1/test/ping
    /// </remarks>
    [HttpGet("ping")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public IActionResult Ping()
    {
        var mode = _cfg["Storage:Mode"] ?? Environment.GetEnvironmentVariable("STORAGE_MODE") ?? "InMemory";
        var dataRoot = _cfg["Storage:DataRoot"] ?? Environment.GetEnvironmentVariable("STORAGE_DATA_ROOT");

        return Ok(new
        {
            ok = true,
            сервис = "PeaceDatabase.WebApi",
            режим = mode,
            каталог = dataRoot,
            времяUtc = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Самопроверка работы с БД (self-check).
    /// </summary>
    /// <remarks>
    /// Алгоритм проверки:
    /// 1. Создаёт указанную БД (если её нет).
    /// 2. Записывает или обновляет документ с id <c>selfcheck-doc</c>.
    /// 3. Читает этот документ обратно.
    /// 4. Если включён файловый режим — показывает путь к каталогу БД на диске.
    ///
    /// Используется для быстрой проверки, что запись и чтение документов работают,
    /// а в файловом режиме данные реально сохраняются на жёсткий диск.
    ///
    /// Пример запроса:
    /// POST /v1/test/selfcheck
    /// {
    ///   "db": "fs_test"
    /// }
    /// </remarks>
    [HttpPost("selfcheck")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(object), StatusCodes.Status500InternalServerError)]
    public IActionResult SelfCheck([FromBody, Required] SelfCheckRequest body)
    {
        var db = body.Db?.Trim();
        if (string.IsNullOrWhiteSpace(db))
        {
            throw new DomainValidationException(
                "The db field is required.",
                new Dictionary<string, string[]>
                {
                    ["db"] = new[] { "The db field is required." }
                });
        }

        ControllerErrorMapper.ThrowIfFailed(_svc.CreateDb(db), db);

        var docId = "selfcheck-doc";
        var existing = _svc.Get(db, docId);
        Document payload;

        if (existing is null)
        {
            payload = new Document { Id = docId, Data = new() { ["k"] = "v1", ["text"] = "Привет, файловый режим!" } };
            payload = ControllerErrorMapper.EnsureDocument(_svc.Post(db, payload), db, docId);
        }
        else
        {
            payload = new Document { Id = existing.Id, Rev = existing.Rev, Data = new() { ["k"] = "v2", ["text"] = "Документ обновлён" } };
            payload = ControllerErrorMapper.EnsureDocument(_svc.Put(db, payload), db, docId);
        }

        var read = _svc.Get(db, docId);
        if (read is null) throw new InvalidOperationException("Self-check document was not readable after write.");

        var mode = _cfg["Storage:Mode"] ?? Environment.GetEnvironmentVariable("STORAGE_MODE") ?? "InMemory";
        string? dataRoot = _cfg["Storage:DataRoot"] ?? Environment.GetEnvironmentVariable("STORAGE_DATA_ROOT");
        string? dbDir = null;

        if (string.Equals(mode, "File", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(dataRoot))
            dbDir = Path.Combine(dataRoot, SanitizeName(db));

        return Ok(new
        {
            ok = true,
            режим = mode,
            база = db,
            каталог = dbDir,
            записано = new { id = payload.Id, rev = payload.Rev },
            прочитано = read,
            seq = _svc.Seq(db)
        });
    }

    /// <summary>
    /// Эхо-метод (echo).
    /// </summary>
    /// <remarks>
    /// Возвращает тот же JSON, который был отправлен в запросе.
    /// Удобно для проверки сериализации/десериализации в API.
    ///
    /// Пример:
    /// POST /v1/test/echo
    /// {
    ///   "hello": "мир"
    /// }
    /// </remarks>
    [HttpPost("echo")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public IActionResult Echo([FromBody] object body) => Ok(new { ok = true, echo = body });

    private static string SanitizeName(string s)
    {
        foreach (var c in Path.GetInvalidFileNameChars())
            s = s.Replace(c, '_');
        return s;
    }

    public sealed class SelfCheckRequest
    {
        /// <summary>Имя базы данных, с которой будет выполняться проверка.</summary>
        [Required] public string? Db { get; set; }
    }
}
