package io.peacedb.spark;

import io.peacedb.spark.read.PeacedbScanBuilder;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PeacedbTable implements Table, SupportsRead {

    public static final StructType SCHEMA = new StructType(new StructField[] {
            new StructField("id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("rev", DataTypes.StringType, true, Metadata.empty()),
            new StructField("deleted", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("data_json", DataTypes.StringType, true, Metadata.empty()),
            new StructField("tags", DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty()),
            new StructField("content", DataTypes.StringType, true, Metadata.empty())
    });

    private final CaseInsensitiveStringMap options;

    public PeacedbTable(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public String name() {
        return "peacedb_table";
    }

    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    public Map<String, String> properties() {
        return Collections.emptyMap();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new PeacedbScanBuilder(options);
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }
}
