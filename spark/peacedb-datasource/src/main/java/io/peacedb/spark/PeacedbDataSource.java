package io.peacedb.spark;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.Transform;

public class PeacedbDataSource implements TableProvider, DataSourceRegister {

    @Override
    public String shortName() {
        return "peacedb";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return PeacedbTable.SCHEMA;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return false;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitions, Map<String, String> properties) {
        // Not used in Spark 3.5; delegate to overload with CaseInsensitiveStringMap
        return new PeacedbTable(new CaseInsensitiveStringMap(properties));
    }

    // Spark 3.5 uses getTable(StructType, Transform[], Map<String, String>)
}
