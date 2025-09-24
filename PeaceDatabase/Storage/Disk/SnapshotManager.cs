// File: Storage/Disk/SnapshotManager.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using PeaceDatabase.Storage.Disk.Internals;

namespace PeaceDatabase.Storage.Disk
{
    /// <summary>
    /// Сохранение/чтение снапшотов (JSON Lines).
    /// Для простоты: снапшот - список документов с системными полями.
    /// </summary>
    internal sealed class SnapshotManager
    {
        private readonly string _dbDir;
        private readonly StorageOptions _opt;

        public SnapshotManager(string dbDir, StorageOptions opt)
        {
            Directory.CreateDirectory(dbDir);
            _dbDir = dbDir;
            _opt = opt;
        }
        public bool TryReadManifest(out Manifest? manifest)
        {
            var manifestPath = Path.Combine(_dbDir, _opt.ManifestFileName);
            if (!File.Exists(manifestPath)) { manifest = null; return false; }
            try
            {
                manifest = JsonSerializer.Deserialize<Manifest>(File.ReadAllText(manifestPath));
                return manifest != null;
            }
            catch { manifest = null; return false; }
        }
        public string CreateSnapshot<TDoc>(IEnumerable<TDoc> docs, long lastSeq)
        {
            var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var fileName = $"{_opt.SnapshotPrefix}{ts}{_opt.SnapshotExt}";
            var path = Path.Combine(_dbDir, fileName);

            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
            using var sw = new StreamWriter(fs, Encoding.UTF8);

            foreach (var doc in docs)
                sw.WriteLine(JsonSerializer.Serialize(doc));

            sw.Flush();
            fs.Flush(true);

            // Обновляем manifest
            var manifest = new Manifest { LastSeq = lastSeq, ActiveSnapshot = fileName, SnapshotTimeUtc = DateTimeOffset.UtcNow };
            File.WriteAllText(Path.Combine(_dbDir, _opt.ManifestFileName),
                              JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true }));

            return path;
        }

        public IEnumerable<string> ReadActiveSnapshotLines()
        {
            var manifestPath = Path.Combine(_dbDir, _opt.ManifestFileName);
            if (!File.Exists(manifestPath))
                yield break;

            Manifest? man = null;
            try
            {
                man = JsonSerializer.Deserialize<Manifest>(File.ReadAllText(manifestPath));
            }
            catch { /* игнорируем битый manifest */ }

            if (man == null || string.IsNullOrWhiteSpace(man.ActiveSnapshot))
                yield break;

            var path = Path.Combine(_dbDir, man.ActiveSnapshot);
            if (!File.Exists(path))
                yield break;

            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(fs, Encoding.UTF8);
            string? line;
            while ((line = sr.ReadLine()) != null)
                if (!string.IsNullOrWhiteSpace(line))
                    yield return line;
        }

        public sealed class Manifest
        {
            public long LastSeq { get; set; }
            public string? ActiveSnapshot { get; set; }
            public DateTimeOffset SnapshotTimeUtc { get; set; }
        }
    }
}
