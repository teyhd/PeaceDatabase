using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Client;

/// <summary>
/// HTTP-клиент для связи с удалённым шардом.
/// Использует стандартный REST API PeaceDatabase.
/// </summary>
public sealed class HttpShardClient : IShardClient
{
    private readonly HttpClient _http;
    private readonly ILogger<HttpShardClient>? _logger;
    private readonly JsonSerializerOptions _jsonOpts;
    private bool _disposed;

    public ShardInfo ShardInfo { get; }

    public HttpShardClient(ShardInfo shardInfo, HttpClient httpClient, ILogger<HttpShardClient>? logger = null)
    {
        ShardInfo = shardInfo ?? throw new ArgumentNullException(nameof(shardInfo));
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;

        _jsonOpts = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = true
        };
    }

    public async Task<bool> HealthCheckAsync(CancellationToken ct = default)
    {
        try
        {
            var response = await _http.GetAsync($"{ShardInfo.BaseUrl}/healthz", ct);
            var healthy = response.IsSuccessStatusCode;
            ShardInfo.Status = healthy ? ShardStatus.Healthy : ShardStatus.Unhealthy;
            ShardInfo.LastHealthCheck = DateTimeOffset.UtcNow;
            return healthy;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Health check failed for shard {ShardId}", ShardInfo.Id);
            ShardInfo.Status = ShardStatus.Unhealthy;
            return false;
        }
    }

    // --- Database operations ---

    public async Task<(bool Ok, string? Error)> CreateDbAsync(string db, CancellationToken ct = default)
    {
        try
        {
            var response = await _http.PutAsync($"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}", null, ct);
            if (response.IsSuccessStatusCode)
                return (true, null);

            var error = await ReadErrorAsync(response, ct);
            return (false, error);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    public async Task<(bool Ok, string? Error)> DeleteDbAsync(string db, CancellationToken ct = default)
    {
        try
        {
            var response = await _http.DeleteAsync($"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}", ct);
            if (response.IsSuccessStatusCode)
                return (true, null);

            var error = await ReadErrorAsync(response, ct);
            return (false, error);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    // --- Document CRUD ---

    public async Task<Document?> GetAsync(string db, string id, string? rev = null, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(id)}";
            if (!string.IsNullOrEmpty(rev))
                url += $"?rev={Uri.EscapeDataString(rev)}";

            var response = await _http.GetAsync(url, ct);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<Document>(_jsonOpts, ct);
        }
        catch
        {
            return null;
        }
    }

    public async Task<(bool Ok, Document? Doc, string? Error)> PutAsync(string db, Document doc, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(doc.Id)}";
            var response = await _http.PutAsJsonAsync(url, doc, _jsonOpts, ct);

            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<Document>(_jsonOpts, ct);
                return (true, result, null);
            }

            var error = await ReadErrorAsync(response, ct);
            return (false, null, error);
        }
        catch (Exception ex)
        {
            return (false, null, ex.Message);
        }
    }

    public async Task<(bool Ok, Document? Doc, string? Error)> PostAsync(string db, Document doc, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs";
            var response = await _http.PostAsJsonAsync(url, doc, _jsonOpts, ct);

            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<Document>(_jsonOpts, ct);
                return (true, result, null);
            }

            var error = await ReadErrorAsync(response, ct);
            return (false, null, error);
        }
        catch (Exception ex)
        {
            return (false, null, ex.Message);
        }
    }

    public async Task<(bool Ok, string? Error)> DeleteAsync(string db, string id, string rev, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(id)}?rev={Uri.EscapeDataString(rev)}";
            var response = await _http.DeleteAsync(url, ct);

            if (response.IsSuccessStatusCode)
                return (true, null);

            var error = await ReadErrorAsync(response, ct);
            return (false, error);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    // --- Queries ---

    public async Task<IReadOnlyList<Document>> AllDocsAsync(string db, int skip = 0, int limit = 1000, bool includeDeleted = true, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_all_docs?skip={skip}&limit={limit}&includeDeleted={includeDeleted}";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
                return Array.Empty<Document>();

            var result = await response.Content.ReadFromJsonAsync<AllDocsResult>(_jsonOpts, ct);
            return (IReadOnlyList<Document>?)result?.Items ?? Array.Empty<Document>();
        }
        catch
        {
            return Array.Empty<Document>();
        }
    }

    public async Task<int> SeqAsync(string db, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_seq";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
                return 0;

            var result = await response.Content.ReadFromJsonAsync<SeqResult>(_jsonOpts, ct);
            return result?.Seq ?? 0;
        }
        catch
        {
            return 0;
        }
    }

    public async Task<StatsDto> StatsAsync(string db, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_stats";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
                return new StatsDto { Db = db };

            return await response.Content.ReadFromJsonAsync<StatsDto>(_jsonOpts, ct) ?? new StatsDto { Db = db };
        }
        catch
        {
            return new StatsDto { Db = db };
        }
    }

    // --- Search ---

    public async Task<IReadOnlyList<Document>> FindByFieldsAsync(
        string db,
        IDictionary<string, string>? equals = null,
        (string field, double? min, double? max)? numericRange = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_find/fields";
            var body = new
            {
                EqualsMap = equals,
                NumericField = numericRange?.field,
                Min = numericRange?.min,
                Max = numericRange?.max,
                Skip = skip,
                Limit = limit
            };

            var response = await _http.PostAsJsonAsync(url, body, _jsonOpts, ct);
            if (!response.IsSuccessStatusCode)
                return Array.Empty<Document>();

            var result = await response.Content.ReadFromJsonAsync<AllDocsResult>(_jsonOpts, ct);
            return (IReadOnlyList<Document>?)result?.Items ?? Array.Empty<Document>();
        }
        catch
        {
            return Array.Empty<Document>();
        }
    }

    public async Task<IReadOnlyList<Document>> FindByTagsAsync(
        string db,
        IEnumerable<string>? allOf = null,
        IEnumerable<string>? anyOf = null,
        IEnumerable<string>? noneOf = null,
        int skip = 0,
        int limit = 100,
        CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_find/tags";
            var body = new
            {
                AllOf = allOf,
                AnyOf = anyOf,
                NoneOf = noneOf,
                Skip = skip,
                Limit = limit
            };

            var response = await _http.PostAsJsonAsync(url, body, _jsonOpts, ct);
            if (!response.IsSuccessStatusCode)
                return Array.Empty<Document>();

            var result = await response.Content.ReadFromJsonAsync<AllDocsResult>(_jsonOpts, ct);
            return (IReadOnlyList<Document>?)result?.Items ?? Array.Empty<Document>();
        }
        catch
        {
            return Array.Empty<Document>();
        }
    }

    public async Task<IReadOnlyList<Document>> FullTextSearchAsync(string db, string query, int skip = 0, int limit = 100, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ShardInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_search?q={Uri.EscapeDataString(query)}&skip={skip}&limit={limit}";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
                return Array.Empty<Document>();

            var result = await response.Content.ReadFromJsonAsync<AllDocsResult>(_jsonOpts, ct);
            return (IReadOnlyList<Document>?)result?.Items ?? Array.Empty<Document>();
        }
        catch
        {
            return Array.Empty<Document>();
        }
    }

    // --- Helpers ---

    private async Task<string> ReadErrorAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            var content = await response.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(content);
            if (doc.RootElement.TryGetProperty("error", out var errorProp))
                return errorProp.GetString() ?? response.ReasonPhrase ?? "Unknown error";
            return response.ReasonPhrase ?? "Unknown error";
        }
        catch
        {
            return response.ReasonPhrase ?? "Unknown error";
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // HttpClient управляется извне, не диспозим
    }

    // --- Response DTOs ---

    private sealed class AllDocsResult
    {
        public int Total { get; set; }
        public List<Document>? Items { get; set; }
    }

    private sealed class SeqResult
    {
        public string? Db { get; set; }
        public int Seq { get; set; }
    }
}

