## PeaceDB Spark DataSource (Java) and Job

Build requires JDK 11+, Maven, Spark 3.5.1. PeaceDatabase Web API must run at `http://localhost:5000`.

Data source features:
- Plans partitions using fast `/v1/db/{db}/_stats` (no page probing for large datasets)
- Configurable timeouts and automatic HTTP retries

### Build

```
mvn -q -DskipTests package
```

Artifacts:
- `peacedb-datasource/target/peacedb-datasource-*.jar`
- `peacedb-job/target/peacedb-job-*.jar`

### Run job

```
spark-submit \
  --class io.peacedb.job.PeacedbToColumnar \
  --master local[*] \
  peacedb-job/target/peacedb-job-*.jar \
  --baseUrl http://localhost:5000 \
  --db news \
  --outDir ./out \
  --pageSize 200 \
  --connectTimeoutMs 10000 \
  --readTimeoutMs 60000 \
  --retries 3 \
  --retryBackoffMs 500
```

Outputs: `out/parquet_*`, `out/orc_*`, and `out/metrics.csv` with size and timing.

Connector options (Spark.read.format("peacedb")):
- `baseUrl` (default `http://localhost:5000`)
- `db` (default `news`)
- `pageSize` (default `100`)
- `includeDeleted` (default `false`)
- `maxRows` (for benchmarking)
- `connectTimeoutMs` / `readTimeoutMs`
- `retries` / `retryBackoffMs`


