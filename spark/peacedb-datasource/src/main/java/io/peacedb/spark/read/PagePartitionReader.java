package io.peacedb.spark.read;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.peacedb.spark.http.ApiClient;
import io.peacedb.spark.model.Document;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.connector.read.PartitionReader;

public class PagePartitionReader implements PartitionReader<InternalRow> {
    private final ApiClient client;
    private final StructType schema;
    private final PageInputPartition part;
    private final ObjectMapper mapper = new ObjectMapper();

    private Iterator<Document> iter;
    private Document current;

    public PagePartitionReader(ApiClient client, StructType schema, PageInputPartition part) {
        this.client = client;
        this.schema = schema;
        this.part = part;

        List<Document> docs = client.fetchPage(part.db, part.skip, part.limit, part.includeDeleted);
        this.iter = docs.iterator();
    }

    @Override
    public boolean next() throws IOException {
        if (iter.hasNext()) {
            current = iter.next();
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        final Object[] values = new Object[6];
        values[0] = UTF8String.fromString(current.id);
        values[1] = current.rev == null ? null : UTF8String.fromString(current.rev);
        values[2] = current.deleted;
        String dataJson = null;
        try {
            if (current.data != null) {
                dataJson = mapper.writeValueAsString(current.data);
            }
        } catch (JsonProcessingException e) {
            dataJson = null;
        }
        values[3] = dataJson == null ? null : UTF8String.fromString(dataJson);
        if (current.tags != null) {
            UTF8String[] arr = current.tags.stream().map(UTF8String::fromString).toArray(UTF8String[]::new);
            values[4] = new org.apache.spark.sql.catalyst.util.GenericArrayData(arr);
        } else {
            values[4] = null;
        }
        values[5] = current.content == null ? null : UTF8String.fromString(current.content);
        return new GenericInternalRow(values);
    }

    @Override
    public void close() throws IOException {
        // nothing
    }
}
