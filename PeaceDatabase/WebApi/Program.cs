// File: WebApi/Program.cs
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Routing.Patterns;

using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.InMemory;

// ===== HDD mode (файловое хранилище)
using PeaceDatabase.Storage.Disk;
using PeaceDatabase.Storage.Disk.Internals;

var builder = WebApplication.CreateBuilder(args);

// ---------- JSON ----------
builder.Services
    .AddControllers()
    .AddJsonOptions(o =>
    {
        o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        o.JsonSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
        o.JsonSerializerOptions.WriteIndented = false;
        o.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });

// ---------- Логирование ----------
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// ---------- Режим хранения (InMemory | File) ----------
string storageMode =
    builder.Configuration["Storage:Mode"]
    ?? Environment.GetEnvironmentVariable("STORAGE_MODE")
    ?? "InMemory";

storageMode = storageMode.Equals("File", StringComparison.OrdinalIgnoreCase) ? "File" : "InMemory";

// Настройки файлового хранилища (используются только при File)
var dataRoot = builder.Configuration["Storage:DataRoot"]
               ?? Environment.GetEnvironmentVariable("STORAGE_DATA_ROOT")
               ?? Path.Combine(AppContext.BaseDirectory, "data");

var storageOptions = new StorageOptions
{
    DataRoot = dataRoot,
    EnableSnapshots = true,
    SnapshotEveryNOperations = 500, // под себя
    SnapshotMaxWalSizeMb = 64,
    Durability = DurabilityLevel.Commit
};

// ---------- DI ----------
if (storageMode == "File")
{
    builder.Services.AddSingleton<IDocumentService>(_ => new FileDocumentService(storageOptions));
}
else
{
    builder.Services.AddSingleton<IDocumentService, InMemoryDocumentService>();
}

// ---------- Health / CORS / Swagger ----------
builder.Services.AddHealthChecks();

const string CorsPolicy = "DefaultCors";
builder.Services.AddCors(opt =>
{
    opt.AddPolicy(CorsPolicy, p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod());
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// ---------- Метрики ----------
var meter = new Meter("PeaceDatabase.WebApi", "1.0.0");
var reqCounter = meter.CreateCounter<long>("http.requests.total", unit: "count", description: "Total HTTP requests");
var reqDuration = meter.CreateHistogram<double>("http.requests.duration.ms", unit: "ms", description: "HTTP request duration");

// ADD: совместимость с тестами
var requestsTotal = 0L;

// ---------- Build ----------
var app = builder.Build();

// ---------- Глобальная обработка ошибок ----------
app.UseExceptionHandler(errorApp =>
{
    errorApp.Run(async context =>
    {
        var feature = context.Features.Get<IExceptionHandlerFeature>();
        var problem = new ProblemDetails
        {
            Type = "about:blank",
            Title = "Unhandled exception",
            Detail = app.Environment.IsDevelopment() ? feature?.Error.ToString() : feature?.Error.Message,
            Status = StatusCodes.Status500InternalServerError
        };
        context.Response.ContentType = "application/problem+json";
        context.Response.StatusCode = problem.Status ?? 500;
        await context.Response.WriteAsJsonAsync(problem);
    });
});

app.UseHttpsRedirection();
app.UseCors(CorsPolicy);

// ---------- Логирование запросов + метрики ----------
app.Use(async (context, next) =>
{
    var sw = Stopwatch.StartNew();
    var logger = context.RequestServices.GetRequiredService<ILogger<Program>>();

    Interlocked.Increment(ref requestsTotal);
    reqCounter.Add(1,
        new KeyValuePair<string, object?>("method", context.Request.Method),
        new KeyValuePair<string, object?>("path", context.Request.Path.Value ?? string.Empty));

    try { await next(); }
    finally
    {
        sw.Stop();
        var elapsedMs = sw.Elapsed.TotalMilliseconds;

        reqDuration.Record(elapsedMs,
            new KeyValuePair<string, object?>("method", context.Request.Method),
            new KeyValuePair<string, object?>("path", context.Request.Path.Value ?? string.Empty),
            new KeyValuePair<string, object?>("status", context.Response.StatusCode));

        logger.LogInformation("HTTP {Method} {Path} -> {Status} in {Elapsed:0.0} ms",
            context.Request.Method, context.Request.Path.Value, context.Response.StatusCode, elapsedMs);
    }
});

// ---------- Swagger только в Dev ----------
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapControllers();
app.MapHealthChecks("/healthz");

// Лёгкая служебная сводка (совместимость с тестами)
app.MapGet("/v1/_stats", () => Results.Ok(new
{
    requestsTotal,
    service = "PeaceDatabase.WebApi",
    version = "1.0.0",
    storageMode,
    timeUtc = DateTime.UtcNow
}));

// =====================================================================
//            R O U T E S   F O R   H A R D   D I S K   M O D E
//    Утилитарные методы, полезные при файловом режиме (и безопасные
//    при InMemory — просто возвращают info, ничего не ломают).
// =====================================================================
app.MapGroup("/v1/_storage")
    .WithTags("_storage")
    .MapStorageEndpoints(storageMode, dataRoot);

// ------------------------------------------------------------
// 1) Подробная схема по данным ApiExplorer (Controllers/Actions)
// ------------------------------------------------------------
app.MapGet("/v1/_api", ([FromServices] IApiDescriptionGroupCollectionProvider provider) =>
{
    var groups = provider.ApiDescriptionGroups.Items;

    var items = groups
        .SelectMany(g => g.Items)
        .Select(d => new
        {
            method = d.HttpMethod ?? "*",
            path = "/" + (d.RelativePath ?? string.Empty).TrimStart('/'),
            group = d.GroupName,
            controller = d.ActionDescriptor.RouteValues.TryGetValue("controller", out var ctrl) ? ctrl : null,
            action = d.ActionDescriptor.RouteValues.TryGetValue("action", out var act) ? act : null,
            parameters = d.ParameterDescriptions.Select(p => new
            {
                name = p.Name,
                source = p.Source?.DisplayName,
                type = p.Type?.FullName,
                required = p.IsRequired,
                @default = p.DefaultValue
            }),
            consumes = d.SupportedRequestFormats?.Select(f => f.MediaType).Distinct().ToArray() ?? Array.Empty<string>(),
            responses = d.SupportedResponseTypes.Select(r => new
            {
                statusCode = r.StatusCode,
                type = r.Type?.FullName,
                formats = r.ApiResponseFormats.Select(f => f.MediaType).Distinct().ToArray()
            })
        })
        .OrderBy(x => x.path)
        .ThenBy(x => x.method)
        .ToArray();

    return Results.Ok(items);
})
.WithName("_api_catalog")
.Produces(StatusCodes.Status200OK)
.WithTags("_introspect");

// ------------------------------------------------------------
// 2) Фактические маршруты из EndpointDataSource (включая Minimal API)
// ------------------------------------------------------------
app.MapGet("/v1/_routes", ([FromServices] EndpointDataSource ds) =>
{
    static string FormatRoutePattern(RoutePattern? pattern)
    {
        if (pattern is null) return "/";
        if (!string.IsNullOrEmpty(pattern.RawText))
            return pattern.RawText!.StartsWith("/") ? pattern.RawText! : "/" + pattern.RawText!;

        var sb = new StringBuilder();
        sb.Append('/');

        for (int i = 0; i < pattern.PathSegments.Count; i++)
        {
            if (i > 0) sb.Append('/');
            var seg = pattern.PathSegments[i];

            foreach (var part in seg.Parts)
            {
                if (part is RoutePatternLiteralPart lit)
                {
                    sb.Append(lit.Content);
                }
                else if (part is RoutePatternParameterPart par)
                {
                    sb.Append('{');
                    if (par.IsCatchAll) sb.Append("**");
                    sb.Append(par.Name);
                    if (par.IsOptional) sb.Append('?');
                    sb.Append('}');
                }
                else
                {
                    sb.Append("{?}");
                }
            }
        }

        return sb.ToString();
    }

    var endpoints = ds.Endpoints
        .OfType<RouteEndpoint>()
        .SelectMany(e =>
        {
            var pattern = FormatRoutePattern(e.RoutePattern);
            var methods = e.Metadata.OfType<IHttpMethodMetadata>().FirstOrDefault()?.HttpMethods ?? new[] { "*" };
            var display = e.DisplayName;

            return methods.Select(m => new
            {
                method = m,
                path = pattern,
                displayName = display
            });
        })
        .OrderBy(x => x.path)
        .ThenBy(x => x.method)
        .ToArray();

    return Results.Ok(endpoints);
})
.WithName("_routes_catalog")
.Produces(StatusCodes.Status200OK)
.WithTags("_introspect");

app.Run();

public partial class Program { }

// ======================
// Extensions
// ======================
static class StorageEndpoints
{
    public static RouteGroupBuilder MapStorageEndpoints(this RouteGroupBuilder group, string storageMode, string dataRoot)
    {
        // GET /v1/_storage/info
        group.MapGet("/info", () =>
        {
            var root = storageMode == "File" ? dataRoot : null;
            var dbDirs = (storageMode == "File" && Directory.Exists(dataRoot))
                ? Directory.GetDirectories(dataRoot).Select(Path.GetFileName).OrderBy(x => x).ToArray()
                : Array.Empty<string>();

            return Results.Ok(new
            {
                mode = storageMode,
                dataRoot = root,
                dbDirs
            });
        })
        .WithName("_storage_info")
        .Produces(StatusCodes.Status200OK);

        // GET /v1/_storage/dir/{db}  -> абсолютный путь к каталогу БД (для «открыть в проводнике» вручную)
        group.MapGet("/dir/{db}", (string db) =>
        {
            if (storageMode != "File")
                return Results.BadRequest(new { ok = false, error = "Storage mode is InMemory" });

            var safe = SanitizeName(db);
            var dir = Path.Combine(dataRoot, safe);
            return Results.Ok(new { db, dir, exists = Directory.Exists(dir) });
        })
        .WithName("_storage_dir")
        .Produces(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest);

        return group;
    }

    private static string SanitizeName(string s)
    {
        foreach (var c in Path.GetInvalidFileNameChars())
            s = s.Replace(c, '_');
        return s;
    }
}
