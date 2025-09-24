// File: PeaceDatabase.Tests/FileDocumentServiceCrashRecoveryTests.cs
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Disk;
using PeaceDatabase.Storage.Disk.Internals;
using Xunit;

namespace PeaceDatabase.Tests
{
    public class FileDocumentServiceCrashRecoveryTests : IAsyncLifetime
    {
        private readonly string _rootDir;
        private readonly StorageOptions _optsNoSnapshot;

        public FileDocumentServiceCrashRecoveryTests()
        {
            _rootDir = Path.Combine(Path.GetTempPath(), "peace-db-tests", $"crash-{Guid.NewGuid():N}");

            // Специальные опции: СНАПШОТЫ ОТКЛЮЧЕНЫ, чтобы проверить чистое восстановление из WAL
            _optsNoSnapshot = new StorageOptions
            {
                DataRoot = _rootDir,
                EnableSnapshots = false,
                SnapshotEveryNOperations = int.MaxValue,
                SnapshotMaxWalSizeMb = 1024,
                Durability = DurabilityLevel.Commit
            };
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public Task DisposeAsync()
        {
            try { if (Directory.Exists(_rootDir)) Directory.Delete(_rootDir, recursive: true); } catch { }
            return Task.CompletedTask;
        }

        [Fact]
        public void Crash_Recovery_From_WAL_Only()
        {
            var db = "t_crash";

            // 1) Первый "сеанс": пишем только WAL (снапшоты выключены)
            using (var svc = new FileDocumentService(_optsNoSnapshot))
            {
                svc.CreateDb(db);

                // последовательность операций: put, put, delete
                var p1 = svc.Post(db, new Document { Id = "id1", Data = new() { ["n"] = 1 } });
                p1.Ok.Should().BeTrue();

                var p2 = svc.Post(db, new Document { Id = "id2", Data = new() { ["n"] = 2 } });
                p2.Ok.Should().BeTrue();

                var head = svc.Get(db, "id2");
                head.Should().NotBeNull();
                var del = svc.Delete(db, "id2", head!.Rev);
                del.Ok.Should().BeTrue();

                // есть wal.log, снапшота нет
                var dbDir = Path.Combine(_rootDir, db);
                File.Exists(Path.Combine(dbDir, "wal.log")).Should().BeTrue();
                Directory.EnumerateFiles(dbDir, "snapshot-*.jsonl").Any().Should().BeFalse();
            } // симулируем "краш": просто dispose без снапшота

            // 2) Второй запуск: сервис должен доиграть WAL и восстановить RAM-состояние
            using (var svc2 = new FileDocumentService(_optsNoSnapshot))
            {
                var alive = svc2.AllDocs(db, includeDeleted: false).ToList();
                alive.Should().ContainSingle(x => x.Id == "id1");
                alive.Should().NotContain(x => x.Id == "id2"); // удалён в WAL

                // Проверим, что Seq поднялся
                svc2.Seq(db).Should().BeGreaterThan(0);
            }
        }

        [Fact]
        public async Task Api_Smoke_With_WAL_Recovery()
        {
            // Готовим данные
            var db = "api_wal";
            using (var svc = new FileDocumentService(_optsNoSnapshot))
            {
                svc.CreateDb(db);
                svc.Post(db, new Document { Id = "a", Data = new() { ["x"] = 1 } });
            }

            // Поднимем тестовый WebAPI поверх того же DataRoot и проверим /v1/_stats
            await using var app = new TestApiFactory(_optsNoSnapshot);
            var client = app.CreateClient();

            var resp = await client.GetAsync("/v1/_stats");
            resp.StatusCode.Should().Be(HttpStatusCode.OK);

            // И убедимся через DI, что данные доступны
            using var scope = app.Services.CreateScope();
            var s = scope.ServiceProvider.GetRequiredService<IDocumentService>();
            var doc = s.Get(db, "a");
            doc.Should().NotBeNull();
           // (int)doc!.Data!["x"]!.Should().Be(1);
            //doc!.Data!["x"].Should().Be(1);
            // В методе Api_Smoke_With_WAL_Recovery():
            AsInt(doc!.Data!["x"]!).Should().Be(1);


        }
        private static int AsInt(object? v)
        {
            if (v is null) throw new InvalidOperationException("Value is null");
            if (v is int i) return i;
            if (v is long l) return checked((int)l);
            if (v is double d) return (int)d;
            if (v is decimal m) return (int)m;
            if (v is System.Text.Json.JsonElement je)
            {
                return je.ValueKind switch
                {
                    System.Text.Json.JsonValueKind.Number => je.TryGetInt32(out var vi)
                        ? vi
                        : (je.TryGetInt64(out var vl) ? checked((int)vl) : (int)je.GetDouble()),
                    System.Text.Json.JsonValueKind.String => int.Parse(je.GetString()!),
                    _ => throw new InvalidCastException($"JsonElement {je.ValueKind} is not a number")
                };
            }
            return Convert.ToInt32(v);
        }

        private static string? AsString(object? v)
        {
            if (v is null) return null;
            if (v is string s) return s;
            if (v is System.Text.Json.JsonElement je)
            {
                return je.ValueKind switch
                {
                    System.Text.Json.JsonValueKind.String => je.GetString(),
                    System.Text.Json.JsonValueKind.Number => je.GetRawText(), // "123"
                    _ => je.ToString()
                };
            }
            return v.ToString();
        }

        private sealed class TestApiFactory : WebApplicationFactory<Program>, IAsyncDisposable
        {
            private readonly StorageOptions _options;
            public TestApiFactory(StorageOptions options) => _options = options;

            protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
            {
                builder.ConfigureServices(services =>
                {
                    var desc = services.SingleOrDefault(d => d.ServiceType == typeof(IDocumentService));
                    if (desc != null) services.Remove(desc);
                    services.AddSingleton<IDocumentService>(_ => new FileDocumentService(_options));
                });
            }
        }
    }
}
