using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Core.Services;

public interface IDocumentService
{
    Document? Get(string db, string id);
    (bool Ok, Document? Doc, string? Error) Put(string db, Document doc);
    (bool Ok, Document? Doc, string? Error) Post(string db, Document doc);
    (bool Ok, string? Error) Delete(string db, string id);
    IEnumerable<Document> AllDocs(string db);
}
