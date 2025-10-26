package io.peacedb.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public class PeacedbToColumnar {
    public static void main(String[] args) throws Exception {
        Arguments a = Arguments.parse(args);

        SparkSession spark = SparkSession.builder()
                .appName("PeacedbToColumnar")
                // Workarounds for Hadoop native lib on Windows when writing to local FS
                .config("spark.hadoop.io.native.lib.available", "false")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
                .getOrCreate();

        int[] limits = new int[] { 1, 10, 100, 1000, 10000, 100000 };
        Path out = Paths.get(a.outDir);
        Files.createDirectories(out);

        Path metricsPath = out.resolve("metrics.csv");
        boolean append = a.appendMetrics;
        boolean writeHeader = true;
        if (append && Files.exists(metricsPath)) {
            writeHeader = false;
        }
        try (FileWriter fw = new FileWriter(metricsPath.toFile(), append == true)) {
            if (writeHeader)
                fw.write("n,format,bytes,write_ms,read_ms,rows\n");

            for (int n : limits) {
                Dataset<Row> df = spark.read()
                        .format("peacedb")
                        .option("baseUrl", a.baseUrl)
                        .option("db", a.db)
                        .option("pageSize", Integer.toString(Math.min(a.pageSize, n)))
                        .option("connectTimeoutMs", Integer.toString(a.connectTimeoutMs))
                        .option("readTimeoutMs", Integer.toString(a.readTimeoutMs))
                        .option("retries", Integer.toString(a.retries))
                        .option("retryBackoffMs", Integer.toString(a.retryBackoffMs))
                        .option("maxRows", Integer.toString(n))
                        .option("includeDeleted", Boolean.toString(a.includeDeleted))
                        .load();

                StructType dataSchema = new StructType()
                        .add("link", DataTypes.StringType)
                        .add("headline", DataTypes.StringType)
                        .add("category", DataTypes.StringType)
                        .add("short_description", DataTypes.StringType)
                        .add("authors", DataTypes.StringType)
                        .add("date", DataTypes.StringType);

                Dataset<Row> tr = df
                        .withColumn("data_struct", functions.from_json(df.col("data_json"), dataSchema))
                        .select(
                                df.col("id"),
                                functions.size(df.col("tags")).alias("tags_count"),
                                functions.length(df.col("content")).alias("content_len"),
                                functions.col("data_struct.link").alias("link"),
                                functions.col("data_struct.headline").alias("headline"),
                                functions.col("data_struct.category").alias("category"),
                                functions.col("data_struct.short_description").alias("short_description"),
                                functions.col("data_struct.authors").alias("authors"),
                                functions.col("data_struct.date").alias("date"));

                Path outParquet = out.resolve("parquet_" + n);
                Path outOrc = out.resolve("orc_" + n);
                Files.createDirectories(outParquet);
                Files.createDirectories(outOrc);

                // Warm-up: cache transformed data and perform tiny writes/reads to pay one-time
                // costs
                tr.persist();
                tr.count();
                Path warmParquet = out.resolve("_warmup_parquet");
                Path warmOrc = out.resolve("_warmup_orc");
                tr.limit(1).write().mode("overwrite").parquet(warmParquet.toString());
                tr.limit(1).write().mode("overwrite").orc(warmOrc.toString());
                spark.read().parquet(warmParquet.toString()).count();
                spark.read().orc(warmOrc.toString()).count();

                long t0 = System.nanoTime();
                tr.write().mode("overwrite").parquet(outParquet.toString());
                long t1 = System.nanoTime();
                tr.write().mode("overwrite").orc(outOrc.toString());
                long t2 = System.nanoTime();

                long parquetSize = folderSize(outParquet);
                long orcSize = folderSize(outOrc);

                long r0 = System.nanoTime();
                long pc = spark.read().parquet(outParquet.toString()).count();
                long r1 = System.nanoTime();
                long oc = spark.read().orc(outOrc.toString()).count();
                long r2 = System.nanoTime();

                String lineParquet = String.format(Locale.ROOT, "%d,parquet,%d,%.3f,%.3f,%d\n",
                        n, parquetSize, (t1 - t0) / 1e6, (r1 - r0) / 1e6, pc);
                String lineOrc = String.format(Locale.ROOT, "%d,orc,%d,%.3f,%.3f,%d\n",
                        n, orcSize, (t2 - t1) / 1e6, (r2 - r1) / 1e6, oc);
                fw.write(lineParquet);
                fw.write(lineOrc);

                tr.unpersist();
            }
        }

        spark.stop();
    }

    private static long folderSize(Path p) throws IOException {
        try (var s = Files.walk(p)) {
            return s.filter(Files::isRegularFile).mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        }
    }

    private static class Arguments {
        String baseUrl = "http://localhost:5000";
        String db = "news";
        String outDir = "./out";
        int pageSize = 100;
        boolean includeDeleted = false;
        boolean appendMetrics = false;
        int connectTimeoutMs = 10000;
        int readTimeoutMs = 60000;
        int retries = 3;
        int retryBackoffMs = 500;

        static Arguments parse(String[] args) {
            Arguments a = new Arguments();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--baseUrl":
                        a.baseUrl = args[++i];
                        break;
                    case "--db":
                        a.db = args[++i];
                        break;
                    case "--outDir":
                        a.outDir = args[++i];
                        break;
                    case "--pageSize":
                        a.pageSize = Integer.parseInt(args[++i]);
                        break;
                    case "--includeDeleted":
                        a.includeDeleted = Boolean.parseBoolean(args[++i]);
                        break;
                    case "--appendMetrics":
                        a.appendMetrics = Boolean.parseBoolean(args[++i]);
                        break;
                    case "--connectTimeoutMs":
                        a.connectTimeoutMs = Integer.parseInt(args[++i]);
                        break;
                    case "--readTimeoutMs":
                        a.readTimeoutMs = Integer.parseInt(args[++i]);
                        break;
                    case "--retries":
                        a.retries = Integer.parseInt(args[++i]);
                        break;
                    case "--retryBackoffMs":
                        a.retryBackoffMs = Integer.parseInt(args[++i]);
                        break;
                    default:
                        // ignore unknown
                        break;
                }
            }
            return a;
        }
    }
}
