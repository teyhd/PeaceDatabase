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

    public ApiClient(String baseUrl, int connectTimeoutMs, int readTimeoutMs) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
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
        int limit = 1; // minimal fetch
        String url = String.format("%s/v1/db/%s/_all_docs?skip=0&limit=%d&includeDeleted=%s", baseUrl, db, limit,
                includeDeleted);
        ensureClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .timeout(Duration.ofMillis(readTimeoutMs))
                .build();
        try {
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("HTTP " + resp.statusCode() + " for " + url);
            }
            AllDocsResponse r = mapper.readValue(resp.body(), AllDocsResponse.class);
            return r.total;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Document> fetchPage(String db, int skip, int limit, boolean includeDeleted) {
        String url = String.format("%s/v1/db/%s/_all_docs?skip=%d&limit=%d&includeDeleted=%s", baseUrl, db, skip,
                Math.max(1, limit),
                includeDeleted);
        ensureClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .timeout(Duration.ofMillis(readTimeoutMs))
                .build();
        try {
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("HTTP " + resp.statusCode() + " for " + url);
            }
            AllDocsResponse r = mapper.readValue(resp.body(), AllDocsResponse.class);
            return r.items;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AllDocsResponse {
        public int total;
        public List<Document> items;
    }
}
