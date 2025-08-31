from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, expr, lit, udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.avro.functions import from_avro
import requests
import json
import glob
import logging
import struct

from dotenv import load_dotenv
load_dotenv()
import requests
import os

    


def clean_logical_types(schema):
    if isinstance(schema, dict):
        schema.pop("logicalType", None)
        for k, v in schema.items():
            clean_logical_types(v)
    elif isinstance(schema, list):
        for item in schema:
            clean_logical_types(item)
def get_schema_str():
    try:
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL")
        topic_name = os.getenv("TOPIC_NAME")

        url = f"{schema_registry_url}/subjects/{topic_name}-value/versions/latest"
        response = requests.get(
            url,
            auth=("user123", "password123"),
            timeout=10
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch schema from registry: {e}")
        raise SystemExit("Stopping Spark job because schema is unavailable.")
    response.raise_for_status()
    schema_response = response.json()
    schema_str = json.loads(schema_response["schema"])
    clean_logical_types(schema_str)
    schema_str = json.dumps(schema_str)
    return schema_str

def decode_avro_binary(avro_df,schema_str):
    # Skip the first 5 bytes (magic byte + schema ID)
    avro_df = avro_df.withColumn("avro_binary", expr("substring(value, 6, length(value)-5)"))

    decoded_df = avro_df.select(from_avro(col("avro_binary"), schema_str).alias("data"))
    flatten_df = decoded_df.select("data.*")
    final_df = flatten_df.withColumn(
        "ingest_time",
        from_unixtime((col("timestamp").cast("long") / 1000)).cast("timestamp")
    )
    return final_df 