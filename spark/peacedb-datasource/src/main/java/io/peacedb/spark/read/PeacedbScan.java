package io.peacedb.spark.read;

import io.peacedb.spark.http.ApiClient;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;

public class PeacedbScan implements Scan, Batch {
    private final StructType schema;
    private final ApiClient client;
    private final String db;
    private final int pageSize;
    private final boolean includeDeleted;
    private final Integer maxRows;

    public PeacedbScan(StructType schema, ApiClient client, String db, int pageSize, boolean includeDeleted,
            Integer maxRows) {
        this.schema = schema;
        this.client = client;
        this.db = db;
        this.pageSize = pageSize;
        this.includeDeleted = includeDeleted;
        this.maxRows = maxRows;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "PeacedbScan(db=" + db + ", pageSize=" + pageSize + ")";
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        long total;
        if (maxRows != null) {
            total = maxRows;
        } else {
            // Prefer fast stats endpoint if available
            try {
                var stats = client.fetchDbStats(db);
                total = includeDeleted ? stats.docsTotal : stats.docsAlive;
            } catch (RuntimeException e) {
                // Fallback: light probe of first page to avoid full scan
                java.util.List<io.peacedb.spark.model.Document> items = client.fetchPage(db, 0, pageSize,
                        includeDeleted);
                total = items != null ? items.size() : 0;
            }
        }

        if (total <= 0) {
            return new InputPartition[0];
        }

        int numPages = (int) ((total + pageSize - 1) / pageSize);
        InputPartition[] parts = new InputPartition[numPages];
        for (int i = 0; i < numPages; i++) {
            int skip = i * pageSize;
            int limit = (int) Math.min(pageSize, Math.max(0, total - skip));
            parts[i] = new PageInputPartition(db, skip, limit, includeDeleted);
        }
        return parts;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new PagePartitionReaderFactory(client, schema);
    }
}
