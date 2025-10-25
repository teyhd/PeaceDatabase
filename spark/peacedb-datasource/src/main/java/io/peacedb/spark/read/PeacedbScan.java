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
            // Probe pages to estimate total since API returns page count in 'total'.
            long counted = 0;
            while (true) {
                int limit = pageSize;
                int skip = (int) counted;
                java.util.List<io.peacedb.spark.model.Document> items = client.fetchPage(db, skip, limit,
                        includeDeleted);
                int got = items != null ? items.size() : 0;
                if (got <= 0)
                    break;
                counted += got;
                if (got < limit)
                    break; // last page
                // Safety cap to avoid excessive planning
                if (counted >= 1_000_000)
                    break;
            }
            total = counted;
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
