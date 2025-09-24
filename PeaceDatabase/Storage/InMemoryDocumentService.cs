using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PeaceDatabase.Core.Services;
using System.Collections.Concurrent;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.InMemory.Indexing;
using PeaceDatabase.Storage.InMemory.Internals;

namespace PeaceDatabase.Storage.InMemory
{
    /// <summary>
    /// InMemory реализация документной БД с N-hash ревизиями, MVCC, индексами и поиском.
    /// Потокобезопасность: ReaderWriterLockSlim на уровне базы.
    /// </summary>
    public class InMemoryDocumentService : IDocumentService
    {
        private readonly Dictionary<string, DbState> _dbs = new(StringComparer.Ordinal);

        // --- DB lifecycle ---
        public (bool Ok, string? Error) CreateDb(string db)
        {
            if (string.IsNullOrWhiteSpace(db)) return (false, "Bad database name");
            lock (_dbs)
            {
                if (_dbs.ContainsKey(db)) return (true, null);
                _dbs[db] = new DbState();
            }
            return (true, null);
        }

        public (bool Ok, string? Error) DeleteDb(string db)
        {
            lock (_dbs)
            {
                if (!_dbs.TryGetValue(db, out var state)) return (false, "Database not found");
                _dbs.Remove(db);
                state.Lock.Dispose();
            }
            return (true, null);
        }

        private bool TryGetDb(string db, out DbState state)
        {
            lock (_dbs) return _dbs.TryGetValue(db, out state!);
        }

        // --- CRUD ---
        public Document? Get(string db, string id, string? rev = null)
        {
            if (!TryGetDb(db, out var st)) return null;
            st.Lock.EnterReadLock();
            try
            {
                if (!st.Heads.TryGetValue(id, out var head)) return null;
                var targetRev = rev ?? head.Rev;
                if (!st.Revs.TryGetValue(id, out var revMap)) return null;
                if (!revMap.TryGetValue(targetRev, out var json)) return null;
                return JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
            }
            finally { st.Lock.ExitReadLock(); }
        }

        public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
        {
            if (!TryGetDb(db, out var st)) return (false, null, "Database not found");
            if (string.IsNullOrWhiteSpace(doc.Id)) return (false, null, "Missing _id");

            st.Lock.EnterWriteLock();
            try
            {
                st.Heads.TryGetValue(doc.Id, out var head);

                // -- проверка ревизии / конфликтов
                // -- проверка ревизии / конфликтов
                if (head != null)
                {
                    if (string.IsNullOrEmpty(doc.Rev) ||
                        !string.Equals(doc.Rev, head.Rev, StringComparison.Ordinal))
                    {
                        return (false, null, "Document update conflict");
                    }
                }
                else
                {
                    if (!string.IsNullOrEmpty(doc.Rev))
                        return (false, null, "New document must not provide _rev");
                }

                // -- старая голова (для переиндексации)
                Document? oldHeadDoc = null;
                if (head != null && st.Revs.TryGetValue(doc.Id, out var oldMap) && oldMap.TryGetValue(head.Rev, out var oldJson))
                    oldHeadDoc = JsonSerializer.Deserialize<Document>(oldJson, JsonUtil.JsonOpts);

                // -- next _rev (N-hash)
                var bodyBytes = JsonUtil.CanonicalBodyBytes(doc);
                var nextRev = HashUtil.NextRev(head?.Rev, bodyBytes);

                // -- запись ревизии
                if (!st.Revs.TryGetValue(doc.Id, out var revMap))
                {
                    revMap = new SortedDictionary<string, string>(StringComparer.Ordinal);
                    st.Revs[doc.Id] = revMap;
                }
                var finalDoc = JsonUtil.DeepClone(doc);
                finalDoc.Rev = nextRev;
                var json = JsonSerializer.Serialize(finalDoc, JsonUtil.JsonOpts);
                revMap[nextRev] = json;

                // -- обновить голову
                st.Heads[doc.Id] = new Head { Rev = nextRev, Deleted = finalDoc.Deleted };

                // -- переиндексация
                if (oldHeadDoc != null) Indexer.Unindex(st, oldHeadDoc);
                if (!finalDoc.Deleted) Indexer.Index(st, finalDoc);

                st.Seq++;

                var ret = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                return (true, ret, null);
            }
            finally { st.Lock.ExitWriteLock(); }
        }

        public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
        {
            if (!TryGetDb(db, out var st)) return (false, null, "Database not found");
            if (string.IsNullOrWhiteSpace(doc.Id))
                doc.Id = HashUtil.NewId();
            return Put(db, doc);
        }

        public (bool Ok, string? Error) Delete(string db, string id, string rev)
        {
            if (!TryGetDb(db, out var st)) return (false, "Database not found");
            if (string.IsNullOrWhiteSpace(id)) return (false, "Missing _id");
            if (string.IsNullOrWhiteSpace(rev)) return (false, "Missing _rev");

            st.Lock.EnterWriteLock();
            try
            {
                if (!st.Heads.TryGetValue(id, out var head)) return (false, "Document not found");
                if (!string.Equals(rev, head.Rev, StringComparison.Ordinal)) return (false, "Document update conflict");

                if (!st.Revs.TryGetValue(id, out var revMap)) return (false, "Document not found");
                if (!revMap.TryGetValue(head.Rev, out var json)) return (false, "Document not found");
                var current = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;

                // снять старую версию из индексов
                Indexer.Unindex(st, current);

                // пометить удалённым и записать новую ревизию
                current.Deleted = true;
                var bodyBytes = JsonUtil.CanonicalBodyBytes(current);
                var nextRev = HashUtil.NextRev(head.Rev, bodyBytes);
                current.Rev = nextRev;

                var newJson = JsonSerializer.Serialize(current, JsonUtil.JsonOpts);
                revMap[nextRev] = newJson;

                st.Heads[id] = new Head { Rev = nextRev, Deleted = true };
                st.Seq++;

                return (true, null);
            }
            finally { st.Lock.ExitWriteLock(); }
        }

        public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true)
        {
            if (!TryGetDb(db, out var st)) yield break;
            if (limit <= 0) yield break;
            if (limit > 1000) limit = 1000;
            if (skip < 0) skip = 0;

            st.Lock.EnterReadLock();
            try
            {
                int taken = 0, skipped = 0;
                foreach (var id in st.Heads.Keys)
                {
                    var head = st.Heads[id];
                    if (!includeDeleted && head.Deleted) continue;

                    if (skipped < skip) { skipped++; continue; }

                    if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                    if (!revMap.TryGetValue(head.Rev, out var json)) continue;

                    yield return JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;

                    taken++;
                    if (taken >= limit) yield break;
                }
            }
            finally { st.Lock.ExitReadLock(); }
        }

        public int Seq(string db)
        {
            if (!TryGetDb(db, out var st)) return 0;
            st.Lock.EnterReadLock();
            try { return st.Seq; }
            finally { st.Lock.ExitReadLock(); }
        }

        // --- Поиск по полям/диапазонам ---
        public IEnumerable<Document> FindByFields(
            string db,
            IDictionary<string, string>? equals = null,
            (string field, double? min, double? max)? numericRange = null,
            int skip = 0,
            int limit = 100)
        {
            if (!TryGetDb(db, out var st)) return Enumerable.Empty<Document>();
            if (limit <= 0) return Enumerable.Empty<Document>();
            if (limit > 1000) limit = 1000;
            if (skip < 0) skip = 0;

            st.Lock.EnterReadLock();
            try
            {
                HashSet<string>? acc = null;

                // equals: пересечение
                if (equals != null && equals.Count > 0)
                {
                    foreach (var (field, val) in equals)
                    {
                        if (!st.EqIndex.TryGetValue(field, out var byVal) || !byVal.TryGetValue(val, out var set))
                        {
                            acc = new HashSet<string>(); break;
                        }
                        acc = acc == null ? new HashSet<string>(set, StringComparer.Ordinal)
                                          : Intersect(acc, set);
                        if (acc.Count == 0) break;
                    }
                }

                // numericRange: один диапазон
                if (numericRange.HasValue)
                {
                    var (field, min, max) = numericRange.Value;
                    var ids = new HashSet<string>(StringComparer.Ordinal);
                    if (st.NumIndex.TryGetValue(field, out var tree))
                    {
                        foreach (var (val, set) in tree)
                        {
                            if (min.HasValue && val < min.Value) continue;
                            if (max.HasValue && val > max.Value) break;
                            foreach (var id in set) ids.Add(id);
                        }
                    }
                    acc = acc == null ? ids : Intersect(acc, ids);
                }

                return Materialize(st, acc, skip, limit);
            }
            finally { st.Lock.ExitReadLock(); }

            static HashSet<string> Intersect(HashSet<string> a, HashSet<string> b)
            {
                if (a.Count > b.Count) (a, b) = (b, a);
                var res = new HashSet<string>(StringComparer.Ordinal);
                foreach (var x in a) if (b.Contains(x)) res.Add(x);
                return res;
            }
        }

        // --- Поиск по тегам ---
        public IEnumerable<Document> FindByTags(
            string db,
            IEnumerable<string>? allOf = null,
            IEnumerable<string>? anyOf = null,
            IEnumerable<string>? noneOf = null,
            int skip = 0,
            int limit = 100)
        {
            if (!TryGetDb(db, out var st)) return Enumerable.Empty<Document>();
            if (limit <= 0) return Enumerable.Empty<Document>();
            if (limit > 1000) limit = 1000;
            if (skip < 0) skip = 0;

            st.Lock.EnterReadLock();
            try
            {
                HashSet<string>? acc = null;

                // ALL: пересечение
                if (allOf != null)
                {
                    foreach (var tag in allOf)
                    {
                        var key = tag.Trim();
                        if (!st.TagIndex.TryGetValue(key, out var set))
                        {
                            acc = new HashSet<string>(); break;
                        }
                        acc = acc == null ? new HashSet<string>(set, StringComparer.Ordinal)
                                          : Intersect(acc, set);
                        if (acc.Count == 0) break;
                    }
                }

                // ANY: объединение, затем пересечение с текущим acc
                if (anyOf != null)
                {
                    var uni = new HashSet<string>(StringComparer.Ordinal);
                    foreach (var tag in anyOf)
                    {
                        var key = tag.Trim();
                        if (st.TagIndex.TryGetValue(key, out var set))
                            foreach (var id in set) uni.Add(id);
                    }
                    acc = acc == null ? uni : Intersect(acc, uni);
                }

                // NONE: исключение из базового множества
                if (noneOf != null)
                {
                    var ban = new HashSet<string>(StringComparer.Ordinal);
                    foreach (var tag in noneOf)
                    {
                        var key = tag.Trim();
                        if (st.TagIndex.TryGetValue(key, out var set))
                            foreach (var id in set) ban.Add(id);
                    }
                    var baseSet = acc ?? new HashSet<string>(st.Heads.Keys, StringComparer.Ordinal);
                    baseSet.ExceptWith(ban);
                    acc = baseSet;
                }

                return Materialize(st, acc, skip, limit);
            }
            finally { st.Lock.ExitReadLock(); }

            static HashSet<string> Intersect(HashSet<string> a, HashSet<string> b)
            {
                if (a.Count > b.Count) (a, b) = (b, a);
                var res = new HashSet<string>(StringComparer.Ordinal);
                foreach (var x in a) if (b.Contains(x)) res.Add(x);
                return res;
            }
        }

        // --- Полнотекст ---
        public IEnumerable<Document> FullTextSearch(string db, string query, int skip = 0, int limit = 100)
        {
            if (!TryGetDb(db, out var st)) return Enumerable.Empty<Document>();
            if (string.IsNullOrWhiteSpace(query)) return Enumerable.Empty<Document>();
            if (limit <= 0) return Enumerable.Empty<Document>();
            if (limit > 1000) limit = 1000;
            if (skip < 0) skip = 0;

            var tokens = Indexing.FullTextTokenizer.Tokenize(query).ToList();
            if (tokens.Count == 0) return Enumerable.Empty<Document>();

            st.Lock.EnterReadLock();
            try
            {
                HashSet<string>? acc = null;
                foreach (var tok in tokens)
                {
                    if (!st.FullText.TryGetValue(tok, out var set))
                    {
                        acc = new HashSet<string>(); break;
                    }
                    acc = acc == null ? new HashSet<string>(set, StringComparer.Ordinal)
                                      : Intersect(acc, set);
                    if (acc.Count == 0) break;
                }

                return Materialize(st, acc, skip, limit);
            }
            finally { st.Lock.ExitReadLock(); }

            static HashSet<string> Intersect(HashSet<string> a, HashSet<string> b)
            {
                if (a.Count > b.Count) (a, b) = (b, a);
                var res = new HashSet<string>(StringComparer.Ordinal);
                foreach (var x in a) if (b.Contains(x)) res.Add(x);
                return res;
            }
        }

        // --- Общий материализатор из множества id в документы-головы ---
        private static IEnumerable<Document> Materialize(DbState st, HashSet<string>? ids, int skip, int limit)
        {
            var finalIds = ids ?? new HashSet<string>(st.Heads.Keys, StringComparer.Ordinal);
            var results = new List<Document>(limit);
            int skipped = 0;

            foreach (var id in finalIds.OrderBy(x => x, StringComparer.Ordinal))
            {
                if (!st.Heads.TryGetValue(id, out var head) || head.Deleted) continue;
                if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                if (!revMap.TryGetValue(head.Rev, out var json)) continue;
                if (skipped < skip) { skipped++; continue; }

                results.Add(JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!);
                if (results.Count >= limit) break;
            }
            return results;
        }
    }
}
