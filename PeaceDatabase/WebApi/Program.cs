// File: Program.cs
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http.Metadata;      // <- для IHttpMethodMetadata
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Routing.Patterns;   // <- для RoutePattern и его частей

using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.InMemory;

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

// ---------- DI ----------
builder.Services.AddSingleton<IDocumentService, InMemoryDocumentService>();

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

    // счетчики
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

// Лёгкая служебная сводка
app.MapGet("/v1/_stats", () => Results.Ok(new
{
    requestsTotal,                // <= ожидается тестом
    service = "PeaceDatabase.WebApi",
    version = "1.0.0",
    timeUtc = DateTime.UtcNow
}));


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
                source = p.Source?.DisplayName,    // route / query / body / header / form
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
//    Без DebuggerToString: реконструируем шаблон вручную
// ------------------------------------------------------------
app.MapGet("/v1/_routes", ([FromServices] EndpointDataSource ds) =>
{
    static string FormatRoutePattern(RoutePattern? pattern)
    {
        if (pattern is null) return "/";
        if (!string.IsNullOrEmpty(pattern.RawText))
        {
            return pattern.RawText!.StartsWith("/") ? pattern.RawText! : "/" + pattern.RawText!;
        }

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
                    sb.Append(par.Name);   // <-- вместо ParameterName
                    if (par.IsOptional) sb.Append('?');
                    sb.Append('}');
                }

                else
                {
                    // на всякий случай — неизвестные части
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

            var methods = e.Metadata
                .OfType<IHttpMethodMetadata>()
                .FirstOrDefault()?.HttpMethods ?? new[] { "*" };

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
