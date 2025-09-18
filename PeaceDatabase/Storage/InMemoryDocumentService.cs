using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;

namespace PeaceDatabase.Storage;

public class InMemoryDocumentService : IDocumentService
{
    private readonly Dictionary<string, Dictionary<string, Document>> _dbs = new();
    private readonly Dictionary<string, int> _seqCounters = new();

    private string NewRev() => Guid.NewGuid().ToString();
    private string NewId() => Guid.NewGuid().ToString();

    // -------------------
    // Управление базами
    // -------------------
    public (bool Ok, string? Error) CreateDb(string db)
    {
        if (_dbs.ContainsKey(db))
            return (true, null); // идемпотентно
        _dbs[db] = new Dictionary<string, Document>();
        _seqCounters[db] = 0;
        return (true, null);
    }

    public (bool Ok, string? Error) DeleteDb(string db)
    {
        if (!_dbs.ContainsKey(db))
            return (false, "Database not found");
        _dbs.Remove(db);
        _seqCounters.Remove(db);
        return (true, null);
    }

    // -------------------
    // CRUD документов
    // -------------------
    public Document? Get(string db, string id)
    {
        if (!_dbs.ContainsKey(db)) return null;
        _dbs[db].TryGetValue(id, out var doc);
        return doc;
    }

    public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
    {
        if (!_dbs.ContainsKey(db)) return (false, null, "Database not found");

        if (_dbs[db].TryGetValue(doc.Id, out var existing))
        {
            if (string.IsNullOrEmpty(doc.Rev) || doc.Rev != existing.Rev)
                return (false, null, "Document update conflict"); // 409
        }

        doc.Rev = NewRev();
        _dbs[db][doc.Id] = doc;
        _seqCounters[db] = _seqCounters.GetValueOrDefault(db, 0) + 1;
        return (true, doc, null);
    }

    public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
    {
        if (!_dbs.ContainsKey(db)) return (false, null, "Database not found");

        if (string.IsNullOrEmpty(doc.Id))
            doc.Id = NewId();

        return Put(db, doc);
    }

    public (bool Ok, string? Error) Delete(string db, string id)
    {
        if (!_dbs.ContainsKey(db) || !_dbs[db].ContainsKey(id))
            return (false, "Document not found");

        var doc = _dbs[db][id];
        doc.Deleted = true;
        doc.Rev = NewRev();
        _seqCounters[db] = _seqCounters.GetValueOrDefault(db, 0) + 1;
        return (true, null);
    }

    // -------------------
    // AllDocs с пагинацией
    // -------------------
    public IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000)
    {
        if (!_dbs.ContainsKey(db)) yield break;
        var docs = _dbs[db].Values.Skip(skip).Take(Math.Min(limit, 1000));
        foreach (var doc in docs)
            yield return doc;
    }

    public int Seq(string db) => _seqCounters.GetValueOrDefault(db, 0);
}
