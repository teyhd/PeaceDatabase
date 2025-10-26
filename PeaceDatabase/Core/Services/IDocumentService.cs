using System.Collections.Generic;
using PeaceDatabase.Core.Models;

namespace PeaceDatabase.Core.Services
{

    public interface IDocumentService
    {
        // ---------- Управление базами ----------
        /// <summary>Создать БД (идемпотентно).</summary>
        (bool Ok, string? Error) CreateDb(string db);

        /// <summary>Удалить БД.</summary>
        (bool Ok, string? Error) DeleteDb(string db);

        // ---------- Документы ----------
        /// <summary>
        /// Получить документ по id (и опционально по rev).
        /// Если <paramref name="rev"/> задан, вернуть конкретную ревизию (если поддерживается реализацией).
        /// </summary>
        Document? Get(string db, string id, string? rev = null);

        /// <summary>
        /// Создать/обновить документ по его _id (upsert).
        /// Если в <paramref name="doc"/> есть _rev, используется оптимистическая блокировка.
        /// </summary>
        (bool Ok, Document? Doc, string? Error) Put(string db, Document doc);

        /// <summary>
        /// Создать документ с авто-генерацией _id.
        /// </summary>
        (bool Ok, Document? Doc, string? Error) Post(string db, Document doc);

        /// <summary>
        /// Удалить документ по id и ревизии.
        /// </summary>
        (bool Ok, string? Error) Delete(string db, string id, string rev);

        /// <summary>
        /// Вернуть список документов (с пагинацией).
        /// </summary>
        IEnumerable<Document> AllDocs(string db, int skip = 0, int limit = 1000, bool includeDeleted = true);

        /// <summary>
        /// Текущая последовательность изменений (аналог update_seq).
        /// </summary>
        int Seq(string db);

        /// <summary>
        /// Статистика по базе (число документов, токены и т.д.).
        /// </summary>
        StatsDto Stats(string db);

        // ---------- Поиск ----------
        /// <summary>
        /// Поиск документов по равенствам полей и (опц.) числовому диапазону.
        /// </summary>
        IEnumerable<Document> FindByFields(
            string db,
            IDictionary<string, string>? equals = null,
            (string field, double? min, double? max)? numericRange = null,
            int skip = 0,
            int limit = 100);

        /// <summary>
        /// Поиск по тегам: все из allOf, хотя бы один из anyOf, исключая noneOf.
        /// </summary>
        IEnumerable<Document> FindByTags(
            string db,
            IEnumerable<string>? allOf = null,
            IEnumerable<string>? anyOf = null,
            IEnumerable<string>? noneOf = null,
            int skip = 0,
            int limit = 100);

        /// <summary>
        /// Полнотекстовый поиск по содержимому документов.
        /// </summary>
        IEnumerable<Document> FullTextSearch(
            string db,
            string query,
            int skip = 0,
            int limit = 100);
    }
}
