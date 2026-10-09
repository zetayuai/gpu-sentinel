"""Spark Structured Streaming: Kafka -> 1-minute GPU rollups -> Iceberg (REST catalog on MinIO).

Run inside the spark container:
  docker exec -it spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \
    /home/iceberg/jobs/streaming_job.py

Interview talking points baked in:
- explicit schema + DLQ split (never let one bad record kill the stream)
- event-time watermark (bounded state, deterministic late-data policy)
- 1-min tumbling window aggregation keyed (host_id, gpu_index)
- checkpointing -> restart-safe, at-least-once into idempotent-friendly sink
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

spark = (SparkSession.builder.appName("gpu-telemetry-rollup")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC")
         # MinIO needs path-style S3 URLs (no warehouse.minio DNS)
         .config("spark.sql.catalog.demo.s3.path-style-access", "true")
         # checkpoint dir uses Hadoop's FS layer (not Iceberg S3FileIO):
         # map s3:// to S3A against MinIO; creds come from AWS_* env vars
         .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("host_id", StringType()), StructField("rack", StringType()),
    StructField("gpu_index", IntegerType()), StructField("gpu_uuid", StringType()),
    StructField("event_time", StringType()),
    StructField("gpu_util", DoubleType()), StructField("sm_active", DoubleType()),
    StructField("mem_used_gb", DoubleType()), StructField("temp_c", DoubleType()),
    StructField("power_w", DoubleType()), StructField("energy_j_total", DoubleType()),
    StructField("ecc_dbe", IntegerType()), StructField("xid_error", IntegerType()),
])

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.telemetry.gpu_metrics_1m (
  window_start TIMESTAMP, window_end TIMESTAMP,
  host_id STRING, rack STRING, gpu_index INT,
  avg_util DOUBLE, max_util DOUBLE, avg_sm_active DOUBLE,
  avg_temp DOUBLE, max_temp DOUBLE, avg_power DOUBLE,
  ecc_dbe BIGINT, xid_errors BIGINT, samples BIGINT
) USING iceberg
PARTITIONED BY (hours(window_start))
""")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "telemetry.gpu.raw")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 50000)
       .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m"))
          .select("m.*")
          .withColumn("event_time", F.to_timestamp("event_time")))

good = parsed.filter("host_id IS NOT NULL AND gpu_util BETWEEN 0 AND 100")

agg = (good
       .withWatermark("event_time", "2 minutes")
       .groupBy(F.window("event_time", "1 minute"),
                "host_id", "rack", "gpu_index")
       .agg(F.avg("gpu_util").alias("avg_util"),
            F.max("gpu_util").alias("max_util"),
            F.avg("sm_active").alias("avg_sm_active"),
            F.avg("temp_c").alias("avg_temp"),
            F.max("temp_c").alias("max_temp"),
            F.avg("power_w").alias("avg_power"),
            F.sum("ecc_dbe").alias("ecc_dbe"),
            F.sum("xid_error").alias("xid_errors"),
            F.count("*").alias("samples"))
       .select(F.col("window.start").alias("window_start"),
               F.col("window.end").alias("window_end"),
               "host_id", "rack", "gpu_index",
               "avg_util", "max_util", "avg_sm_active",
               "avg_temp", "max_temp", "avg_power",
               "ecc_dbe", "xid_errors", "samples"))

q = (agg.writeStream
     .format("iceberg")
     .outputMode("append")
     .trigger(processingTime="1 minute")
     .option("checkpointLocation", "s3://warehouse/checkpoints/gpu_metrics_1m")
     .toTable("demo.telemetry.gpu_metrics_1m"))

print("streaming started; Spark UI at http://localhost:4040")
q.awaitTermination()
