package io.peacedb.spark.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.peacedb.spark.model.Document;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class ApiClient implements Serializable {
    private final String baseUrl;
    private transient HttpClient client;
    private final ObjectMapper mapper;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int retries;
    private final int retryBackoffMs;

    public ApiClient(String baseUrl, int connectTimeoutMs, int readTimeoutMs) {
        this(baseUrl, connectTimeoutMs, readTimeoutMs, 3, 500);
    }

    public ApiClient(String baseUrl, int connectTimeoutMs, int readTimeoutMs, int retries, int retryBackoffMs) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.retries = Math.max(0, retries);
        this.retryBackoffMs = Math.max(0, retryBackoffMs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private void ensureClient() {
        if (this.client == null) {
            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
        }
    }

    public long fetchTotal(String db, boolean includeDeleted) {
        // Backward-compatible light probe (kept as fallback)
        int limit = 1; // minimal fetch
        String url = String.format("%s/v1/db/%s/_all_docs?skip=0&limit=%d&includeDeleted=%s", baseUrl, db, limit,
                includeDeleted);
        String body = sendWithRetry(url);
        try {
            AllDocsResponse r = mapper.readValue(body, AllDocsResponse.class);
            return r.total;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Document> fetchPage(String db, int skip, int limit, boolean includeDeleted) {
        String url = String.format("%s/v1/db/%s/_all_docs?skip=%d&limit=%d&includeDeleted=%s", baseUrl, db, skip,
                Math.max(1, limit),
                includeDeleted);
        String body = sendWithRetry(url);
        try {
            AllDocsResponse r = mapper.readValue(body, AllDocsResponse.class);
            return r.items;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DbStatsResponse fetchDbStats(String db) {
        String url = String.format("%s/v1/db/%s/_stats", baseUrl, db);
        String body = sendWithRetry(url);
        try {
            return mapper.readValue(body, DbStatsResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String sendWithRetry(String url) {
        ensureClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .timeout(Duration.ofMillis(readTimeoutMs))
                .build();
        int attempt = 0;
        while (true) {
            try {
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                int sc = resp.statusCode();
                if (sc == 200)
                    return resp.body();
                // retry on 5xx
                if (attempt < retries && sc >= 500) {
                    backoff(attempt++);
                    continue;
                }
                throw new RuntimeException("HTTP " + sc + " for " + url);
            } catch (IOException | InterruptedException e) {
                if (attempt < retries) {
                    backoff(attempt++);
                    continue;
                }
                throw new RuntimeException(e);
            }
        }
    }

    private void backoff(int attempt) {
        long delay = (long) (retryBackoffMs * Math.max(1, attempt));
        try {
            Thread.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AllDocsResponse {
        public int total;
        public List<Document> items;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DbStatsResponse {
        public String db;
        public int seq;
        public int docsTotal;
        public int docsAlive;
        public int docsDeleted;
    }
}
