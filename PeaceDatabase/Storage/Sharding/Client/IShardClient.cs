using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Client;

/// <summary>
/// Интерфейс клиента для взаимодействия с шардом.
/// Повторяет API IDocumentService, но для удалённого вызова.
/// </summary>
public interface IShardClient : IDisposable
{
    /// <summary>
    /// Информация о шарде.
    /// </summary>
    ShardInfo ShardInfo { get; }

    /// <summary>
    /// Проверка доступности шарда.
    /// </summary>
    Task<bool> HealthCheckAsync(CancellationToken ct = default);

    // --- Database operations ---

    Task<(bool Ok, string? Error)> CreateDbAsync(string db, CancellationToken ct = default);
    Task<(bool Ok, string? Error)> DeleteDbAsync(string db, CancellationToken ct = default);

    // --- Document CRUD ---

    Task<Document?> GetAsync(string db, string id, string? rev = null, CancellationToken ct = default);
    Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc, CancellationToken ct = default);
    Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc, CancellationToken ct = default);
    Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev, CancellationToken ct = default);

    // --- Queries ---

    Task<IReadOnlyList<Document>> AllDocsAsync(string db, int skip = 0, int limit = 1000, bool includeDeleted = true, CancellationToken ct = default);
    Task<int> SeqAsync(string db, CancellationToken ct = default);
    Task<StatsDto> StatsAsync(string db, CancellationToken ct = default);

    // --- Search ---

    Task<IReadOnlyList<Document>> FindByFieldsAsync(
        string db,
        IDictionary<string, string>? equals = null,
        (string field, double? min, double? max)? numericRange = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default);

    Task<IReadOnlyList<Document>> FindByTagsAsync(
        string db,
        IEnumerable<string>? allOf = null,
        IEnumerable<string>? anyOf = null,
        IEnumerable<string>? noneOf = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default);

    Task<IReadOnlyList<Document>> FullTextSearchAsync(string db, string query, int skip = 0, int limit = 100, CancellationToken ct = default);
}

