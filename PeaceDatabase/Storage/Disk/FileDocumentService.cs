// File: Storage/Disk/FileDocumentService.cs
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;
using PeaceDatabase.Storage.Disk.Internals;
using PeaceDatabase.Storage.InMemory;
using PeaceDatabase.Storage.InMemory.Internals;

namespace PeaceDatabase.Storage.Disk
{
    /// <summary>
    /// Дисковая реализация IDocumentService: WAL + снапшоты поверх InMemoryDocumentService.
    /// Порядок записи: WAL -> RAM; периодически создаём снапшот и ротируем WAL.
    /// Восстановление: manifest (lastSeq) -> snapshot -> wal.
    /// </summary>
    public sealed class FileDocumentService : IDocumentService, IDisposable
    {
        private readonly StorageOptions _opt;
        private readonly string _root;
        private readonly InMemoryDocumentService _inner;
        private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);

        private readonly Dictionary<string, WalManager> _wals = new(StringComparer.Ordinal);
        private readonly Dictionary<string, SnapshotManager> _snaps = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _lastSeqByDb = new(StringComparer.Ordinal);

        private sealed record WalRecord(string op, string id, string? rev, long seq, Document? doc, DateTimeOffset ts);

        public FileDocumentService(StorageOptions? options = null)
        {
            _opt = options ?? new StorageOptions();
            _root = _opt.DataRoot;
            Directory.CreateDirectory(_root);

            _inner = new InMemoryDocumentService();
            RecoverAllDatabases();
        }

        // ---------------- DB lifecycle ----------------

        public (bool Ok, string? Error) CreateDb(string db)
        {
            _lock.EnterWriteLock();
            try
            {
                var r = _inner.CreateDb(db);
                if (!r.Ok) return r;
                EnsureDbArtifacts(db);
                return (true, null);
            }
            finally { _lock.ExitWriteLock(); }
        }

        public (bool Ok, string? Error) DeleteDb(string db)
        {
            _lock.EnterWriteLock();
            try
            {
                var r = _inner.DeleteDb(db);
                if (!r.Ok) return r;

                var dir = DbDir(db);
                try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); } catch { /* ignore */ }

                if (_wals.Remove(db, out var wal)) { try { wal.Dispose(); } catch { } }
                _snaps.Remove(db);
                _lastSeqByDb.Remove(db);

                return (true, null);
            }
            finally { _lock.ExitWriteLock(); }
        }

        public IEnumerable<string> ListDbs()
        {
            _lock.EnterReadLock();
            try { return _inner.ListDbs().ToArray(); }
            finally { _lock.ExitReadLock(); }
        }

        // ---------------- CRUD ----------------

        // ВАЖНО: сигнатура возвращает Document? (как в InMemory/IDocumentService)
        public Document? Get(string db, string id, string? rev = null)
        {
            _lock.EnterReadLock();
            try { return _inner.Get(db, id, rev); }
            finally { _lock.ExitReadLock(); }
        }

        public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
        {
            _lock.EnterWriteLock();
            try
            {
                EnsureDbArtifacts(db);
                var r = _inner.Post(db, doc);
                if (!r.Ok || r.Doc is null) return r;

                var seq = NextSeq(db);
                AppendWal(db, new WalRecord("put", r.Doc.Id, r.Doc.Rev, seq, r.Doc, DateTimeOffset.UtcNow));
                MaybeSnapshot(db);
                return (true, r.Doc, null);
            }
            finally { _lock.ExitWriteLock(); }
        }

        public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
        {
            _lock.EnterWriteLock();
            try
            {
                EnsureDbArtifacts(db);
                var r = _inner.Put(db, doc);
                if (!r.Ok || r.Doc is null) return r;

                var seq = NextSeq(db);
                AppendWal(db, new WalRecord("put", r.Doc.Id, r.Doc.Rev, seq, r.Doc, DateTimeOffset.UtcNow));
                MaybeSnapshot(db);
                return (true, r.Doc, null);
            }
            finally { _lock.ExitWriteLock(); }
        }

        public (bool Ok, string? Error) Delete(string db, string id, string? rev)
        {
            _lock.EnterWriteLock();
            try
            {
                EnsureDbArtifacts(db);

                // если rev null/пусто — достаём актуальную голову
                if (string.IsNullOrEmpty(rev))
                {
                    var head = _inner.Get(db, id);
                    if (head == null) return (false, "Document not found");
                    rev = head.Rev;
                }

                // теперь rev гарантированно non-null
                var r = _inner.Delete(db, id, rev!);
                if (!r.Ok) return r;

                var seq = NextSeq(db);
                AppendWal(db, new WalRecord("del", id, rev!, seq, null, DateTimeOffset.UtcNow));
                MaybeSnapshot(db);
                return (true, null);
            }
            finally { _lock.ExitWriteLock(); }
        }

        public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true)
        {
            _lock.EnterReadLock();
            try { return _inner.AllDocs(db, skip, limit, includeDeleted).ToArray(); }
            finally { _lock.ExitReadLock(); }
        }

        public int Seq(string db)
        {
            _lock.EnterReadLock();
            try { return _inner.Seq(db); }
            finally { _lock.ExitReadLock(); }
        }

        // ---------------- Search API ----------------

        public IEnumerable<Document> FindByFields(
            string db,
            IDictionary<string, string>? equals = null,
            (string field, double? min, double? max)? numericRange = null,
            int skip = 0,
            int limit = 100)
        {
            _lock.EnterReadLock();
            try { return _inner.FindByFields(db, equals, numericRange, skip, limit).ToArray(); }
            finally { _lock.ExitReadLock(); }
        }

        public IEnumerable<Document> FindByTags(
            string db,
            IEnumerable<string>? allOf = null,
            IEnumerable<string>? anyOf = null,
            IEnumerable<string>? noneOf = null,
            int skip = 0,
            int limit = 100)
        {
            _lock.EnterReadLock();
            try { return _inner.FindByTags(db, allOf, anyOf, noneOf, skip, limit).ToArray(); }
            finally { _lock.ExitReadLock(); }
        }

        public IEnumerable<Document> FullTextSearch(string db, string query, int skip = 0, int limit = 100)
        {
            _lock.EnterReadLock();
            try { return _inner.FullTextSearch(db, query, skip, limit).ToArray(); }
            finally { _lock.ExitReadLock(); }
        }

        public StatsDto Stats(string db)
        {
            _lock.EnterReadLock();
            try { return _inner.Stats(db); }
            finally { _lock.ExitReadLock(); }
        }

        // ---------------- Recovery ----------------

        private void RecoverAllDatabases()
        {
            foreach (var dbDir in Directory.EnumerateDirectories(_root))
            {
                var db = Path.GetFileName(dbDir);
                if (string.IsNullOrWhiteSpace(db)) continue;

                _inner.CreateDb(db);
                EnsureDbArtifacts(db);

                // 1) lastSeq из manifest
                if (_snaps[db].TryReadManifest(out var man) && man != null)
                    _lastSeqByDb[db] = man.LastSeq;
                else
                    _lastSeqByDb[db] = 0;

                // 2) залить активный снапшот
                foreach (var line in _snaps[db].ReadActiveSnapshotLines())
                {
                    try
                    {
                        var doc = JsonSerializer.Deserialize<Document>(line, JsonUtil.JsonOpts);
                        if (doc is null) continue;
                        _inner.Import(db, doc, setAsHead: true, reindex: true, bumpSeq: false);
                    }
                    catch { /* skip */ }
                }

                // 3) доиграть WAL
                foreach (var line in _wals[db].ReadAllLines())
                {
                    try
                    {
                        var rec = JsonSerializer.Deserialize<WalRecord>(line);
                        if (rec is null) continue;

                        if (string.Equals(rec.op, "put", StringComparison.Ordinal))
                        {
                            if (rec.doc is not null)
                                _inner.Import(db, rec.doc, setAsHead: true, reindex: true, bumpSeq: false);
                        }
                        else if (string.Equals(rec.op, "del", StringComparison.Ordinal))
                        {
                            var rev = rec.rev;
                            if (string.IsNullOrEmpty(rev))
                            {
                                var head = _inner.Get(db, rec.id);
                                if (head != null) rev = head.Rev;
                            }
                            if (!string.IsNullOrEmpty(rev))
                                _inner.Delete(db, rec.id, rev);
                        }

                        if (rec.seq > _lastSeqByDb[db]) _lastSeqByDb[db] = rec.seq;
                    }
                    catch { /* skip */ }
                }
                // FileDocumentService.cs внутри RecoverAllDatabases(), в конце цикла по db:
                _inner.SetSeq(db, (int)_lastSeqByDb[db]);

                // при необходимости можно дернуть пересборку индексов:
                // _inner.RebuildIndexes(db);
            }
        }

        // ---------------- Helpers ----------------

        private void EnsureDbArtifacts(string db)
        {
            if (_wals.ContainsKey(db)) return;

            var dir = DbDir(db);
            Directory.CreateDirectory(dir);
            _wals[db] = new WalManager(dir, _opt);
            _snaps[db] = new SnapshotManager(dir, _opt);

            if (!_lastSeqByDb.ContainsKey(db))
                _lastSeqByDb[db] = 0;
        }

        private string DbDir(string db) => Path.Combine(_root, SanitizeName(db));

        private static string SanitizeName(string s)
        {
            foreach (var c in Path.GetInvalidFileNameChars())
                s = s.Replace(c, '_');
            return s;
        }

        private long NextSeq(string db)
        {
            var next = _lastSeqByDb.TryGetValue(db, out var v) ? v + 1 : 1;
            _lastSeqByDb[db] = next;
            return next;
        }

        private void AppendWal(string db, WalRecord rec)
        {
            if (_wals.TryGetValue(db, out var wal))
                wal.Append(rec);
        }

        private void MaybeSnapshot(string db)
        {
            if (!_opt.EnableSnapshots) return;
            if (!_wals.TryGetValue(db, out var wal) || !_snaps.TryGetValue(db, out var snap)) return;

            var needByOps = _lastSeqByDb[db] > 0 && _lastSeqByDb[db] % _opt.SnapshotEveryNOperations == 0;
            var needBySize = wal.GetSizeBytes() > _opt.SnapshotMaxWalSizeMb * 1024L * 1024L;

            if (!(needByOps || needBySize)) return;

            var docs = _inner.ExportAll(db, includeDeleted: true);
            snap.CreateSnapshot(docs, lastSeq: _lastSeqByDb[db]);
            wal.Rotate();
        }

        public void Dispose()
        {
            foreach (var w in _wals.Values)
            {
                try { w.Dispose(); } catch { }
            }
            _wals.Clear();
        }
    }
}
