using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Core.Services;

public interface IDocumentService
{
    // CRUD документов
    Document? Get(string db, string id);
    (bool Ok, Document? Doc, string? Error) Put(string db, Document doc);
    (bool Ok, Document? Doc, string? Error) Post(string db, Document doc);
    (bool Ok, string? Error) Delete(string db, string id);

    // Получение всех документов с пагинацией
    IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000);

    // Счётчик изменений для _changes
    int Seq(string db);

    // Управление базами данных
    (bool Ok, string? Error) CreateDb(string db);
    (bool Ok, string? Error) DeleteDb(string db);
}
