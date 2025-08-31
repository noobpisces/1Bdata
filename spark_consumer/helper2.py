from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
import requests
import json
import glob
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)

# Load jars dynamically
jars = ",".join(glob.glob("/opt/bitnami/spark/custom-jars/*.jar"))

# Spark Session
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

# Kafka & Schema Registry config
key = "user123"
secret = "password123"
servers = "broker-controller-0:9090,broker-controller-1:9090,broker-0:9090"
schema_registry_address = "http://schema-registry:8082"
subject = "clickstream-topic"

# Get latest Avro schema from Schema Registry
try:
    response = requests.get(f"{schema_registry_address}/subjects/{subject}/versions/latest",
                            auth=(key, secret), timeout=10)
    response.raise_for_status()
    schema_response = response.json()
    schema_dict = json.loads(schema_response["schema"])
except requests.exceptions.RequestException as e:
    logging.error(f"Failed to fetch schema from registry: {e}")
    raise SystemExit("Stopping Spark job because schema is unavailable.")

# Remove all logicalType recursively
def clean_logical_types(schema):
    if isinstance(schema, dict):
        schema.pop("logicalType", None)
        for k, v in schema.items():
            clean_logical_types(v)
    elif isinstance(schema, list):
        for item in schema:
            clean_logical_types(item)

clean_logical_types(schema_dict)

# Convert schema back to string for from_avro()
schema_str = json.dumps(schema_dict)

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", servers) \
    .option("subscribe", "clickstream-topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Decode Avro value
processed_df = raw_df.withColumn(
    "avro_payload",
    expr("substring(value, 6, length(value)-5)")  # Bỏ 5 byte đầu
)

# Giải mã Avro
decoded_df = processed_df.select(
    from_avro(col("avro_payload"), schema_str).alias("data")
)

# Flatten fields
flatten_df = decoded_df.select("data.*")

# Add ingest_time column
final_df = flatten_df.withColumn(
    "ingest_time",
    from_unixtime((col("timestamp").cast("long") / 1000)).cast("timestamp")
)

# Write to Delta Lake on MinIO
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://test/checkpoints/clickstream") \
    .option("failOnMissingMetadata", "false") \
    .start("s3a://test/bronze/clickstream")

query.awaitTermination()
