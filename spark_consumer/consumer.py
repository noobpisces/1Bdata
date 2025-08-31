from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
import requests
import json
import glob
import logging
from dotenv import load_dotenv
from helper import clean_logical_types, get_schema_str, decode_avro_binary
# Logging setup
logging.basicConfig(level=logging.INFO)
load_dotenv()
# Load jars dynamically
jars = ",".join(glob.glob("/opt/bitnami/spark/custom-jars/*.jar"))

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

schema_str = get_schema_str()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker-controller-0:9090,broker-controller-1:9090,broker-0:9090") \
    .option("subscribe", "clickstream-topic") \
    .option("startingOffsets", "earliest") \
    .load() 

final_df = decode_avro_binary(df,schema_str)
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://test/delta/events/_checkpoints/streaming-queries") \
    .start("s3a://test/delta/events")

query.awaitTermination()