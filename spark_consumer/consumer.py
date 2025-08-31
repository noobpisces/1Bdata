# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_unixtime
# from pyspark.sql.avro.functions import from_avro
# from pyspark.sql.functions import col, expr
# import requests
# import json
# import glob
# import logging
# from dotenv import load_dotenv
# from helper import clean_logical_types, get_schema_str, decode_avro_binary
# # Logging setup
# logging.basicConfig(level=logging.INFO)
# load_dotenv()
# # Load jars dynamically
# jars = ",".join(glob.glob("/opt/bitnami/spark/custom-jars/*.jar"))

# spark = SparkSession.builder.appName("SparkConsumer") \
#     .master("spark://spark-master:7077") \
#     .config("spark.jars", jars) \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minio123") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.sql.warehouse.dir", "s3a://test/") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
#     .config("delta.enable-non-concurrent-writes", "true") \
#     .getOrCreate()

# schema_str = get_schema_str()

# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "broker-controller-0:9090,broker-controller-1:9090,broker-0:9090") \
#     .option("subscribe", "clickstream-topic") \
#     .option("startingOffsets", "earliest") \
#     .load() 

# final_df = decode_avro_binary(df,schema_str)
# query = final_df.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "s3a://test/delta/events/_checkpoints/streaming-queries") \
#     .start("s3a://test/delta/events")

# query.awaitTermination()
# consumer.py
import os
import glob
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
jars = ",".join(glob.glob("/opt/bitnami/spark/custom-jars/*.jar"))

from helper import get_schema_str_from_registry, decode_avro_value

# Load .env (tiện chạy local; khi deploy prod có thể truyền env qua spark-submit)
load_dotenv()

TOPIC          = os.getenv("TOPIC_NAME", "clickstream-topic")
BROKERS        = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SEC_PROTOCOL   = os.getenv("SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECH      = os.getenv("SASL_MECHANISMS", "")
SASL_USER      = os.getenv("SASL_USERNAME", "")
SASL_PASS      = os.getenv("SASL_PASSWORD", "")
CLIENT_ID      = os.getenv("CLIENT_ID", "clickstream-consumer")

MAX_OFFSETS    = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
STARTING       = os.getenv("STARTING_OFFSETS", "latest")
TRIGGER_SEC    = int(os.getenv("TRIGGER_SEC", "5"))
WATERMARK      = os.getenv("WATERMARK_DELAY", "24 hours")

CHK_PATH       = os.getenv("CHK_PATH", "file:///tmp/spark/chk/clickstream")
SINK_PATH      = os.getenv("SINK_PATH", "file:///tmp/spark/output/clickstream")

spark = SparkSession.builder.appName("SparkConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars", jars) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio123") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", "s3a://test/") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("delta.enable-non-concurrent-writes", "true") \
    .getOrCreate()

# 1) Fetch schema từ Schema Registry
schema_str = get_schema_str_from_registry(TOPIC)

# 2) Build Kafka options
kopts = {
    "kafka.bootstrap.servers": "broker-controller-0:9090,broker-controller-1:9090,broker-0:9090",
    "subscribe": TOPIC,
    "startingOffsets": STARTING,
    "maxOffsetsPerTrigger": str(MAX_OFFSETS),
    "failOnDataLoss": "false",
    "kafka.client.id": CLIENT_ID
}

# Bật bảo mật nếu cần
if SEC_PROTOCOL and SEC_PROTOCOL != "PLAINTEXT":
    kopts["kafka.security.protocol"] = SEC_PROTOCOL

if SASL_MECH:
    kopts["kafka.sasl.mechanism"] = SASL_MECH

if SASL_USER and SASL_PASS:
    kopts["kafka.sasl.jaas.config"] = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{SASL_USER}" password="{SASL_PASS}";'
    )

# 3) ReadStream từ Kafka
raw = spark.readStream.format("kafka")
for k, v in kopts.items():
    raw = raw.option(k, v)
raw = raw.load()

# 4) Decode Avro → chuẩn hoá
decoded = decode_avro_value(raw, schema_str, timestamp_field="timestamp") \
    .withColumn("dt", to_date(col("event_time")))

# 5) (Tuỳ) watermark để kiểm soát late data
with_wm = decoded.withWatermark("event_time", WATERMARK)

# 6) Ghi ra Parquet (append). Bạn có thể đổi sang Iceberg: .format("iceberg") + .option("path", ...) --> .table("catalog.schema.table")
# query = (
#     with_wm.writeStream
#     .format("parquet")
#     .option("path", SINK_PATH)
#     .option("checkpointLocation", CHK_PATH)
#     .partitionBy("dt")
#     .outputMode("append")
#     .trigger(Trigger.ProcessingTime(f"{TRIGGER_SEC} seconds"))
#     .start()
# )
query = with_wm.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://test/delta/events/_checkpoints/streaming-queries") \
    .start("s3a://test/delta/events")
query.awaitTermination()
