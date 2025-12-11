using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using PeaceDatabase.Core.Models;
using PeaceDatabase.Storage.Sharding.Client;
using PeaceDatabase.Storage.Sharding.Configuration;

namespace PeaceDatabase.Storage.Sharding.Replication.Client;

/// <summary>
/// HTTP-клиент для связи с репликой. Расширяет функциональность HttpShardClient
/// методами репликации и управления.
/// </summary>
public sealed class HttpReplicaClient : IReplicaClient
{
    private readonly HttpClient _http;
    private readonly ILogger<HttpReplicaClient>? _logger;
    private readonly JsonSerializerOptions _jsonOpts;
    private bool _disposed;

    public ReplicaInfo ReplicaInfo { get; }

    public ShardInfo ShardInfo => new()
    {
        Id = ReplicaInfo.ShardId,
        BaseUrl = ReplicaInfo.BaseUrl,
        IsLocal = ReplicaInfo.IsLocal,
        Status = ReplicaInfo.HealthStatus switch
        {
            ReplicaHealthStatus.Healthy => ShardStatus.Healthy,
            ReplicaHealthStatus.Unhealthy => ShardStatus.Unhealthy,
            ReplicaHealthStatus.Initializing => ShardStatus.Initializing,
            _ => ShardStatus.Unknown
        }
    };

    public HttpReplicaClient(ReplicaInfo replicaInfo, HttpClient httpClient, ILogger<HttpReplicaClient>? logger = null)
    {
        ReplicaInfo = replicaInfo ?? throw new ArgumentNullException(nameof(replicaInfo));
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;

        _jsonOpts = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = true
        };
    }

    #region IShardClient Implementation (delegated)

    public async Task<bool> HealthCheckAsync(CancellationToken ct = default)
    {
        try
        {
            var response = await _http.GetAsync($"{ReplicaInfo.BaseUrl}/healthz", ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<(bool Ok, string? Error)> CreateDbAsync(string db, CancellationToken ct = default)
    {
        try
        {
            var response = await _http.PutAsync($"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}", null, ct);
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
            var response = await _http.DeleteAsync($"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}", ct);
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

    public async Task<Document?> GetAsync(string db, string id, string? rev = null, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(id)}";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(doc.Id)}";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/docs/{Uri.EscapeDataString(id)}?rev={Uri.EscapeDataString(rev)}";
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

    public async Task<IReadOnlyList<Document>> AllDocsAsync(string db, int skip = 0, int limit = 1000, bool includeDeleted = true, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_all_docs?skip={skip}&limit={limit}&includeDeleted={includeDeleted}";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_seq";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_stats";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_find/fields";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_find/tags";
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
            var url = $"{ReplicaInfo.BaseUrl}/v1/db/{Uri.EscapeDataString(db)}/_search?q={Uri.EscapeDataString(query)}&skip={skip}&limit={limit}";
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

    #endregion

    #region IReplicaClient Implementation

    public async Task<ReplicationState> GetReplicationStateAsync(CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/state";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
            {
                return new ReplicationState { IsHealthy = false };
            }

            var result = await response.Content.ReadFromJsonAsync<ReplicationStateDto>(_jsonOpts, ct);
            return new ReplicationState
            {
                IsHealthy = result?.Healthy ?? false,
                IsPrimary = result?.IsPrimary ?? false,
                Seq = result?.Seq ?? 0,
                WalPosition = result?.WalPosition,
                Uptime = TimeSpan.FromSeconds(result?.UptimeSeconds ?? 0),
                CurrentPrimaryUrl = result?.CurrentPrimaryUrl,
                ReplicationLag = result?.ReplicationLag ?? 0,
                LastSyncAt = result?.LastSyncAt
            };
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("Failed to get replication state from {Url}: {Error}", ReplicaInfo.BaseUrl, ex.Message);
            return new ReplicationState { IsHealthy = false };
        }
    }

    public async Task<ReplicateResult> ReplicateAsync(ReplicationEntry entry, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/replicate";
            var response = await _http.PostAsJsonAsync(url, entry, _jsonOpts, ct);

            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<ReplicateResultDto>(_jsonOpts, ct);
                return ReplicateResult.Success(result?.Seq ?? entry.Seq);
            }

            var error = await ReadErrorAsync(response, ct);
            return ReplicateResult.Failure(error);
        }
        catch (Exception ex)
        {
            return ReplicateResult.Failure(ex.Message);
        }
    }

    public async Task<ReplicateBatchResult> ReplicateBatchAsync(IEnumerable<ReplicationEntry> entries, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/replicate-batch";
            var response = await _http.PostAsJsonAsync(url, entries, _jsonOpts, ct);

            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<ReplicateBatchResultDto>(_jsonOpts, ct);
                return new ReplicateBatchResult
                {
                    Ok = result?.Ok ?? false,
                    SuccessCount = result?.SuccessCount ?? 0,
                    FailedCount = result?.FailedCount ?? 0,
                    LastSeq = result?.LastSeq ?? 0,
                    Errors = result?.Errors ?? new Dictionary<long, string>()
                };
            }

            var error = await ReadErrorAsync(response, ct);
            return new ReplicateBatchResult { Ok = false, Errors = { { 0, error } } };
        }
        catch (Exception ex)
        {
            return new ReplicateBatchResult { Ok = false, Errors = { { 0, ex.Message } } };
        }
    }

    public async Task PromoteAsync(CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/promote";
            var response = await _http.PostAsync(url, null, ct);

            if (!response.IsSuccessStatusCode)
            {
                var error = await ReadErrorAsync(response, ct);
                _logger?.LogWarning("Failed to promote replica {Url}: {Error}", ReplicaInfo.BaseUrl, error);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("Failed to promote replica {Url}: {Error}", ReplicaInfo.BaseUrl, ex.Message);
        }
    }

    public async Task SetPrimaryAsync(string newPrimaryUrl, CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/set-primary";
            var body = new { PrimaryUrl = newPrimaryUrl };
            var response = await _http.PostAsJsonAsync(url, body, _jsonOpts, ct);

            if (!response.IsSuccessStatusCode)
            {
                var error = await ReadErrorAsync(response, ct);
                _logger?.LogWarning("Failed to set primary for replica {Url}: {Error}", ReplicaInfo.BaseUrl, error);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("Failed to set primary for replica {Url}: {Error}", ReplicaInfo.BaseUrl, ex.Message);
        }
    }

    public async Task<IReadOnlyList<ReplicationEntry>> GetWalEntriesAsync(
        string db,
        long fromSeq,
        int limit = 1000,
        CancellationToken ct = default)
    {
        try
        {
            var url = $"{ReplicaInfo.BaseUrl}/v1/_replication/wal/{Uri.EscapeDataString(db)}?fromSeq={fromSeq}&limit={limit}";
            var response = await _http.GetAsync(url, ct);

            if (!response.IsSuccessStatusCode)
                return Array.Empty<ReplicationEntry>();

            var result = await response.Content.ReadFromJsonAsync<WalEntriesResult>(_jsonOpts, ct);
            return (IReadOnlyList<ReplicationEntry>?)result?.Entries ?? Array.Empty<ReplicationEntry>();
        }
        catch
        {
            return Array.Empty<ReplicationEntry>();
        }
    }

    #endregion

    #region Helpers

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

    #endregion

    #region DTOs

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

    private sealed class ReplicationStateDto
    {
        public bool Healthy { get; set; }
        public bool IsPrimary { get; set; }
        public long Seq { get; set; }
        public string? WalPosition { get; set; }
        public double UptimeSeconds { get; set; }
        public string? CurrentPrimaryUrl { get; set; }
        public long ReplicationLag { get; set; }
        public DateTimeOffset? LastSyncAt { get; set; }
    }

    private sealed class ReplicateResultDto
    {
        public bool Ok { get; set; }
        public long Seq { get; set; }
        public string? Error { get; set; }
    }

    private sealed class ReplicateBatchResultDto
    {
        public bool Ok { get; set; }
        public int SuccessCount { get; set; }
        public int FailedCount { get; set; }
        public long LastSeq { get; set; }
        public Dictionary<long, string>? Errors { get; set; }
    }

    private sealed class WalEntriesResult
    {
        public List<ReplicationEntry>? Entries { get; set; }
    }

    #endregion
}

