// File: PeaceDatabase.Tests/FileDocumentServicePersistenceTests.cs
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
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
    public class FileDocumentServicePersistenceTests : IAsyncLifetime
    {
        private readonly string _rootDir;
        private readonly StorageOptions _opts;
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
        public FileDocumentServicePersistenceTests()
        {
            _rootDir = Path.Combine(Path.GetTempPath(), "peace-db-tests", Guid.NewGuid().ToString("N"));
            _opts = new StorageOptions
            {
                DataRoot = _rootDir,
                EnableSnapshots = true,
                SnapshotEveryNOperations = 3, // понижен порог, чтобы точно случился снапшот
                SnapshotMaxWalSizeMb = 1,
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
        public void Persistence_WritesSnapshot_And_Manifest_And_Reloads()
        {
            var db = "t_persist";

            // 1) Первый запуск: создаём сервис, пишем данные
            using (var svc = new FileDocumentService(_opts))
            {
                var r1 = svc.CreateDb(db);
                r1.Ok.Should().BeTrue();

                // 3 операции => должен сработать триггер снапшота
                var d1 = new Document { Id = "a1", Data = new() { ["name"] = "Alpha" } };
                var p1 = svc.Post(db, d1);
                p1.Ok.Should().BeTrue();

                var d2 = new Document { Id = "a2", Data = new() { ["name"] = "Beta" } };
                var p2 = svc.Post(db, d2);
                p2.Ok.Should().BeTrue();

                var g1 = svc.Get(db, "a1");
                g1.Should().NotBeNull();
                var upd = new Document { Id = g1!.Id, Rev = g1.Rev, Data = new() { ["name"] = "Alpha v2" } };
                var u1 = svc.Put(db, upd);
                u1.Ok.Should().BeTrue();

                // Проверки файлов на диске
                var dbDir = Path.Combine(_rootDir, db);
                Directory.Exists(dbDir).Should().BeTrue();
                File.Exists(Path.Combine(dbDir, _opts.ManifestFileName)).Should().BeTrue();
                Directory.EnumerateFiles(dbDir, $"{_opts.SnapshotPrefix}*{_opts.SnapshotExt}").Any().Should().BeTrue("snapshot must be created");
                File.Exists(Path.Combine(dbDir, _opts.WalFileName)).Should().BeTrue();
            } // dispose

            // 2) Второй запуск: сервис должен восстановить состояние из snapshot + wal
            using (var svc2 = new FileDocumentService(_opts))
            {
                var all = svc2.AllDocs(db).ToList();
                all.Should().ContainSingle(x => x.Id == "a1" && AsString(x.Data!["name"]) == "Alpha v2");
                all.Should().ContainSingle(x => x.Id == "a2" && AsString(x.Data!["name"]) == "Beta");

                // seq должен быть > 0 (загружен из manifest или доигран из WAL)
                svc2.Seq(db).Should().BeGreaterThan(0);
            }
        }

        [Fact]
        public async Task Api_Smoke_Uses_FileDocumentService_DI_And_Returns_Stats()
        {
            // Настраиваем тестовый хост так, чтобы WebAPI использовал FileDocumentService с нашим data-root
            await using var app = new TestApiFactory(_opts);
            var client = app.CreateClient();

            // Просто дымовая проверка, что эндпоинт жив
            var resp = await client.GetAsync("/v1/_stats");
            resp.StatusCode.Should().Be(HttpStatusCode.OK);

            // Создадим базу и документ через DI (напрямую)
            using var scope = app.Services.CreateScope();
            var svc = scope.ServiceProvider.GetRequiredService<IDocumentService>();

            var r = svc.CreateDb("api_db");
            r.Ok.Should().BeTrue();

            var p = svc.Post("api_db", new Document { Id = "x1", Data = new() { ["v"] = 1 } });
            p.Ok.Should().BeTrue();

            // ещё одна проверка HTTP – если есть контроллеры для чтения, можно добавить:
            // var docResp = await client.GetAsync("/v1/doc/api_db/x1");
            // docResp.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // using System.Runtime.InteropServices;
        // using FluentAssertions;

        [Fact(DisplayName = "Open DB folder in OS file explorer (opt-in)")]
        public void Open_Db_Folder_In_FileExplorer()
        {
            // По умолчанию просто выходим — тест пройдёт молча
            var wantOpen = Environment.GetEnvironmentVariable("OPEN_DB_EXPLORER") == "1";
            if (!wantOpen) return;

            var db = "open_me";
            var root = Path.Combine(Path.GetTempPath(), "peace-db-tests", "manual-open");
            var opts = new StorageOptions { DataRoot = root, EnableSnapshots = true };

            using (var svc = new FileDocumentService(opts))
            {
                svc.CreateDb(db);
                svc.Post(db, new Document { Id = "show", Data = new() { ["k"] = "v" } });
            }

            var dbDir = Path.Combine(root, db);
            Directory.Exists(dbDir).Should().BeTrue();

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("explorer.exe", $"\"{dbDir}\"")
                { UseShellExecute = true });
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                System.Diagnostics.Process.Start("open", dbDir);
            }
            else
            {
                System.Diagnostics.Process.Start("xdg-open", dbDir);
            }
        }

        // ---- Test host that injects FileDocumentService into WebAPI ----
        private sealed class TestApiFactory : WebApplicationFactory<Program>, IAsyncDisposable
        {
            private readonly StorageOptions _options;
            public TestApiFactory(StorageOptions options) => _options = options;

            protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
            {
                builder.ConfigureServices(services =>
                {
                    // Снести любую регистрацию IDocumentService (InMemory) и поставить FileDocumentService
                    var desc = services.SingleOrDefault(d => d.ServiceType == typeof(IDocumentService));
                    if (desc != null) services.Remove(desc);

                    services.AddSingleton<IDocumentService>(_ => new FileDocumentService(_options));
                });
            }
        }
    }
}
