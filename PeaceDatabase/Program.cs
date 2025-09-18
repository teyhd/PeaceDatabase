using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IDocumentService, InMemoryDocumentService>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// TODO: change basic logging to normal logging
var requestsTotal = 0;
app.Use(async (context, next) =>
{
    requestsTotal++;
    Console.WriteLine($"[{DateTime.Now}] {context.Request.Method} {context.Request.Path}");
    await next.Invoke();
});

app.MapControllers();

// Metrics
app.MapGet("/v1/_stats", () => new { requestsTotal });

app.Run();
