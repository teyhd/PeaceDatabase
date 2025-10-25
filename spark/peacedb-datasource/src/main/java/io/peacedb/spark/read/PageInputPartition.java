package io.peacedb.spark.read;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class PageInputPartition implements InputPartition, Serializable {
    public final String db;
    public final int skip;
    public final int limit;
    public final boolean includeDeleted;

    public PageInputPartition(String db, int skip, int limit, boolean includeDeleted) {
        this.db = db;
        this.skip = skip;
        this.limit = limit;
        this.includeDeleted = includeDeleted;
    }
}
