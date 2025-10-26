package io.peacedb.spark.read;

import io.peacedb.spark.PeacedbTable;
import io.peacedb.spark.http.ApiClient;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class PeacedbScanBuilder implements ScanBuilder {
    private final CaseInsensitiveStringMap options;

    public PeacedbScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public Scan build() {
        final String baseUrl = options.getOrDefault("baseUrl", "http://localhost:5000");
        final String db = options.getOrDefault("db", "news");
        final int pageSize = Integer.parseInt(options.getOrDefault("pageSize", "100"));
        final boolean includeDeleted = Boolean.parseBoolean(options.getOrDefault("includeDeleted", "false"));
        final Integer maxRows = options.containsKey("maxRows") ? Integer.parseInt(options.get("maxRows")) : null;
        final int connectTimeoutMs = Integer.parseInt(options.getOrDefault("connectTimeoutMs", "5000"));
        final int readTimeoutMs = Integer.parseInt(options.getOrDefault("readTimeoutMs", "15000"));
        final int retries = Integer.parseInt(options.getOrDefault("retries", "3"));
        final int retryBackoffMs = Integer.parseInt(options.getOrDefault("retryBackoffMs", "500"));

        ApiClient client = new ApiClient(baseUrl, connectTimeoutMs, readTimeoutMs, retries, retryBackoffMs);
        return new PeacedbScan(PeacedbTable.SCHEMA, client, db, pageSize, includeDeleted, maxRows);
    }

    public StructType readSchema() {
        return PeacedbTable.SCHEMA;
    }
}
