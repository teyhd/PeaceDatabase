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
                if (!_dbs.TryGetValue(db, out var state))
                {
                    // сделать идемпотентным: окей, «и так нет»
                    return (true, null);
                }
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

                var doc = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                return doc.Deleted ? null : doc; // <-- скрываем удалённые
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

        // InMemoryDocumentService.cs
        public void SetSeq(string db, int value)
        {
            if (!TryGetDb(db, out var st)) return;
            st.Lock.EnterWriteLock();
            try { st.Seq = value < 0 ? 0 : value; }
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
                // Базовая вселенная: все не удалённые ids
                HashSet<string> universe = new(st.Heads.Count, StringComparer.Ordinal);
                foreach (var kv in st.Heads)
                    if (!kv.Value.Deleted) universe.Add(kv.Key);

                HashSet<string>? acc = null;

                // ---- equals: пересечение по каждому полю
                if (equals != null && equals.Count > 0)
                {
                    foreach (var (field, want) in equals)
                    {
                        // 1) Пытаемся через индекс равенств
                        HashSet<string>? byIdx = null;
                        if (st.EqIndex.TryGetValue(field, out var byVal) && byVal.TryGetValue(want, out var setEq))
                        {
                            byIdx = new HashSet<string>(setEq, StringComparer.Ordinal);
                        }
                        else
                        {
                            // 2) Фолбэк: линейный фильтр по текущей "вселенной"/кандидатам
                            var scanBase = acc ?? universe;
                            byIdx = new HashSet<string>(StringComparer.Ordinal);
                            foreach (var id in scanBase)
                            {
                                if (!st.Heads.TryGetValue(id, out var head)) continue;
                                if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                                if (!revMap.TryGetValue(head.Rev, out var json)) continue;

                                var doc = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                                if (doc.Data != null && doc.Data.TryGetValue(field, out var val))
                                {
                                    // сравнение как строк — нормализуем обе стороны
                                    var s = Indexing.Indexer.IndexerString(val);
                                    if (string.Equals(s, want, StringComparison.Ordinal))
                                        byIdx.Add(id);
                                }
                            }
                        }

                        acc = acc == null ? byIdx : Intersect(acc, byIdx);
                        if (acc.Count == 0) break;
                    }
                }

                // ---- numericRange: пересечение с диапазоном
                if (numericRange.HasValue)
                {
                    var (field, min, max) = numericRange.Value;

                    // 1) через числовой индекс
                    HashSet<string>? byNum = null;
                    if (st.NumIndex.TryGetValue(field, out var tree))
                    {
                        byNum = new HashSet<string>(StringComparer.Ordinal);
                        foreach (var kv in tree)
                        {
                            var val = kv.Key;
                            if (min.HasValue && val < min.Value) continue;
                            if (max.HasValue && val > max.Value) break;
                            foreach (var id in kv.Value) byNum.Add(id);
                        }
                    }
                    else
                    {
                        // 2) фолбэк сканом
                        var scanBase = acc ?? universe;
                        byNum = new HashSet<string>(StringComparer.Ordinal);
                        foreach (var id in scanBase)
                        {
                            if (!st.Heads.TryGetValue(id, out var head)) continue;
                            if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                            if (!revMap.TryGetValue(head.Rev, out var json)) continue;

                            var doc = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                            if (doc.Data != null && doc.Data.TryGetValue(field, out var raw) &&
                                Indexing.Indexer.TryToDouble(raw, out var num))
                            {
                                if (min.HasValue && num < min.Value) continue;
                                if (max.HasValue && num > max.Value) continue;
                                byNum.Add(id);
                            }
                        }
                    }

                    acc = acc == null ? byNum : Intersect(acc, byNum);
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
        public void Import(string db, Document doc, bool setAsHead = true, bool reindex = true, bool bumpSeq = false)
        {
            if (!TryGetDb(db, out var st)) return;
            if (string.IsNullOrWhiteSpace(doc.Id)) return;
            if (string.IsNullOrWhiteSpace(doc.Rev)) return;

            st.Lock.EnterWriteLock();
            try
            {
                // Завести карту ревизий, если её ещё нет
                if (!st.Revs.TryGetValue(doc.Id, out var revMap))
                {
                    revMap = new SortedDictionary<string, string>(StringComparer.Ordinal);
                    st.Revs[doc.Id] = revMap;
                }

                // Если есть текущая голова и она была проиндексирована — снять её из индексов (мы импортируем "как есть")
                Document? oldHeadDoc = null;
                if (setAsHead && st.Heads.TryGetValue(doc.Id, out var head)
                    && st.Revs.TryGetValue(doc.Id, out var oldMap)
                    && oldMap.TryGetValue(head.Rev, out var oldJson))
                {
                    oldHeadDoc = JsonSerializer.Deserialize<Document>(oldJson, JsonUtil.JsonOpts);
                }

                if (oldHeadDoc != null) Indexer.Unindex(st, oldHeadDoc);

                // Сохранить ревизию "как есть"
                var json = JsonSerializer.Serialize(doc, JsonUtil.JsonOpts);
                revMap[doc.Rev] = json;

                if (setAsHead)
                    st.Heads[doc.Id] = new Head { Rev = doc.Rev, Deleted = doc.Deleted };

                // Проиндексировать, если документ "живой"
                if (reindex && !doc.Deleted)
                    Indexer.Index(st, doc);

                if (bumpSeq)
                    st.Seq++;
            }
            finally { st.Lock.ExitWriteLock(); }
        }

        /// <summary>
        /// Экспорт всех «голов» (по умолчанию без удалённых) — удобно для снапшота.
        /// </summary>
        public IReadOnlyList<Document> ExportAll(string db, bool includeDeleted = false)
        {
            var list = new List<Document>(capacity: 1024);
            if (!TryGetDb(db, out var st)) return list;

            st.Lock.EnterReadLock();
            try
            {
                foreach (var kv in st.Heads)
                {
                    var id = kv.Key;
                    var head = kv.Value;
                    if (!includeDeleted && head.Deleted) continue;

                    if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                    if (!revMap.TryGetValue(head.Rev, out var json)) continue;

                    var doc = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                    list.Add(doc);
                }
            }
            finally { st.Lock.ExitReadLock(); }

            return list;
        }
        public IEnumerable<string> ListDbs()
        {
            lock (_dbs) return _dbs.Keys.ToList();
        }

        public StatsDto Stats(string db)
        {
            var dto = new StatsDto { Db = db };
            if (!TryGetDb(db, out var st)) return dto;
            st.Lock.EnterReadLock();
            try
            {
                dto.Seq = st.Seq;
                dto.DocsTotal = st.Heads.Count;
                dto.DocsAlive = st.Heads.Values.Count(h => !h.Deleted);
                dto.DocsDeleted = dto.DocsTotal - dto.DocsAlive;
                dto.EqIndexFields = st.EqIndex.Count;
                dto.TagIndexCount = st.TagIndex.Count;
                dto.FullTextTokens = st.FullText.Count;
                return dto;
            }
            finally { st.Lock.ExitReadLock(); }
        }

        /// <summary>
        /// Полная пересборка индексов для базы из актуальных «голов».
        /// Быстро починяет индексы после «глухого» восстановления.
        /// </summary>
        public void RebuildIndexes(string db)
        {
            if (!TryGetDb(db, out var st)) return;

            st.Lock.EnterWriteLock();
            try
            {
                // Сбросить все индексы
                st.EqIndex.Clear();
                st.NumIndex.Clear();
                st.TagIndex.Clear();
                st.FullText.Clear();

                // Проиндексировать заново все актуальные «головы»
                foreach (var kv in st.Heads)
                {
                    var id = kv.Key;
                    var head = kv.Value;
                    if (head.Deleted) continue;

                    if (!st.Revs.TryGetValue(id, out var revMap)) continue;
                    if (!revMap.TryGetValue(head.Rev, out var json)) continue;

                    var doc = JsonSerializer.Deserialize<Document>(json, JsonUtil.JsonOpts)!;
                    Indexer.Index(st, doc);
                }
            }
            finally { st.Lock.ExitWriteLock(); }
        }

        /// <summary>
        /// Унифицированный адаптер: полнотекст в формате (ok, docs, error),
        /// чтобы вызывать как _inner.FullText(db, q, limit, offset).
        /// </summary>
        public (bool Ok, IReadOnlyList<Document> Docs, string? Error) FullText(string db, string query, int limit = 50, int offset = 0)
        {
            try
            {
                var docs = FullTextSearch(db, query, skip: offset, limit: limit).ToList();
                return (true, docs, null);
            }
            catch (Exception ex)
            {
                return (false, Array.Empty<Document>(), ex.Message);
            }
        }

        /// <summary>
        /// Унифицированный адаптер: Find в формате (ok, docs, error),
        /// чтобы вызывать как _inner.Find(db, equals/диапазон, limit, offset).
        /// </summary>
        public (bool Ok, IReadOnlyList<Document> Docs, string? Error) Find(
            string db,
            IDictionary<string, string>? equals = null,
            (string field, double? min, double? max)? numericRange = null,
            int limit = 100,
            int offset = 0)
        {
            try
            {
                var docs = FindByFields(db, equals, numericRange, skip: offset, limit: limit).ToList();
                return (true, docs, null);
            }
            catch (Exception ex)
            {
                return (false, Array.Empty<Document>(), ex.Message);
            }
        }
    }
}
