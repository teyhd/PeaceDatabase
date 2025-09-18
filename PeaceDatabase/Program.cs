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

app.MapControllers();

app.Run();
