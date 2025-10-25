package io.peacedb.spark.read;

import io.peacedb.spark.http.ApiClient;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import java.io.Serializable;

public class PagePartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private final ApiClient client;
    private final StructType schema;

    public PagePartitionReaderFactory(ApiClient client, StructType schema) {
        this.client = client;
        this.schema = schema;
    }

    @Override
    public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(InputPartition partition) {
        PageInputPartition p = (PageInputPartition) partition;
        return new PagePartitionReader(client, schema, p);
    }
}
