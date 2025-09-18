using PeaceDatabase.Core.Models;
using PeaceDatabase.Core.Services;

namespace PeaceDatabase.Storage;

public class InMemoryDocumentService : IDocumentService
{
    private readonly Dictionary<string, Dictionary<string, Document>> _dbs = new();

    public Document? Get(string db, string id)
    {
        if (!_dbs.ContainsKey(db)) return null;
        _dbs[db].TryGetValue(id, out var doc);
        return doc;
    }

    public (bool Ok, Document? Doc, string? Error) Put(string db, Document doc)
    {
        if (!_dbs.ContainsKey(db)) _dbs[db] = new Dictionary<string, Document>();
        doc.Rev = Guid.NewGuid().ToString();
        _dbs[db][doc.Id] = doc;
        return (true, doc, null);
    }

    public (bool Ok, Document? Doc, string? Error) Post(string db, Document doc)
    {
        doc.Id = Guid.NewGuid().ToString();
        return Put(db, doc);
    }

    public (bool Ok, string? Error) Delete(string db, string id)
    {
        if (!_dbs.ContainsKey(db) || !_dbs[db].ContainsKey(id))
            return (false, "Not found");
        _dbs[db].Remove(id);
        return (true, null);
    }

    public IEnumerable<Document> AllDocs(string db)
    {
        if (!_dbs.ContainsKey(db)) yield break;
        foreach (var doc in _dbs[db].Values)
            yield return doc;
    }
}
