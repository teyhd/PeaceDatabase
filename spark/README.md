## PeaceDB Spark DataSource (Java) and Job

Build requires JDK 11+, Maven, Spark 3.5.1. PeaceDatabase Web API must run at `http://localhost:5000`.

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
  --pageSize 200
```

Outputs: `out/parquet`, `out/orc`, and `out/metrics.csv` with size and timing.


