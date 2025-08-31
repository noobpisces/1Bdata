
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_unixtime, expr, lit, udf
# from pyspark.sql.types import IntegerType, StringType
# from pyspark.sql.avro.functions import from_avro
# import requests
# import json
# import glob
# import logging
# import struct

# from dotenv import load_dotenv
# load_dotenv()
# import requests
# import os

    


# def clean_logical_types(schema):
#     if isinstance(schema, dict):
#         schema.pop("logicalType", None)
#         for k, v in schema.items():
#             clean_logical_types(v)
#     elif isinstance(schema, list):
#         for item in schema:
#             clean_logical_types(item)
# def get_schema_str():
#     try:
#         schema_registry_url = os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL")
#         topic_name = os.getenv("TOPIC_NAME")

#         url = f"{schema_registry_url}/subjects/{topic_name}-value/versions/latest"
#         response = requests.get(
#             url,
#             auth=("user123", "password123"),
#             timeout=10
#         )
#         response.raise_for_status()
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Failed to fetch schema from registry: {e}")
#         raise SystemExit("Stopping Spark job because schema is unavailable.")
#     response.raise_for_status()
#     schema_response = response.json()
#     schema_str = json.loads(schema_response["schema"])
#     clean_logical_types(schema_str)
#     schema_str = json.dumps(schema_str)
#     return schema_str

# def decode_avro_binary(avro_df,schema_str):
#     # Skip the first 5 bytes (magic byte + schema ID)
#     avro_df = avro_df.withColumn("avro_binary", expr("substring(value, 6, length(value)-5)"))

#     decoded_df = avro_df.select(from_avro(col("avro_binary"), schema_str).alias("data"))
#     flatten_df = decoded_df.select("data.*")
#     final_df = flatten_df.withColumn(
#         "ingest_time",
#         from_unixtime((col("timestamp").cast("long") / 1000)).cast("timestamp")
#     )
#     return final_df 

# helper.py
import os, json, time, logging, requests
from pyspark.sql.functions import col, from_unixtime, udf
from pyspark.sql.types import BinaryType
from pyspark.sql.avro.functions import from_avro

# ---------- Logging gọn ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- HTTP with retry ----------
def _http_get_with_retry(url, auth=None, headers=None, timeout=10, max_retries=5, backoff=1.5):
    last_err = None
    for i in range(max_retries):
        try:
            r = requests.get(url, auth=auth, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            last_err = e
            sleep = backoff ** i
            logging.warning(f"[SR] GET {url} failed (attempt {i+1}/{max_retries}): {e}. Retry in {sleep:.1f}s")
            time.sleep(sleep)
    logging.error(f"[SR] Exhausted retries for {url}: {last_err}")
    raise SystemExit("Stopping Spark job because schema is unavailable.")

# ---------- Clean logicalTypes (để Spark-Avro đỡ kén) ----------
def clean_logical_types(schema):
    if isinstance(schema, dict):
        schema.pop("logicalType", None)
        for v in schema.values():
            clean_logical_types(v)
    elif isinstance(schema, list):
        for item in schema:
            clean_logical_types(item)

# ---------- Subject name ----------
def build_subject(topic, record_fullname=None, strategy="TopicNameStrategy", is_key=False):
    suffix = "key" if is_key else "value"
    if strategy == "RecordNameStrategy":
        if not record_fullname:
            raise ValueError("RecordNameStrategy cần RECORD_FULLNAME (namespace.name)")
        return record_fullname
    if strategy == "TopicRecordNameStrategy":
        if not record_fullname:
            raise ValueError("TopicRecordNameStrategy cần RECORD_FULLNAME (namespace.name)")
        return f"{topic}-{record_fullname}-{suffix}"
    # TopicNameStrategy (default)
    return f"{topic}-{suffix}"

# ---------- Lấy Avro schema string từ Schema Registry ----------
def get_schema_str_from_registry(topic):
    sr_url   = os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL", "http://localhost:8082").rstrip("/")
    sr_user  = os.getenv("SCHEMA_REGISTRY_USER")
    sr_pass  = os.getenv("SCHEMA_REGISTRY_PASS")
    strategy = os.getenv("SR_SUBJECT_STRATEGY", "TopicNameStrategy")
    record_fullname = os.getenv("RECORD_FULLNAME")

    subject = build_subject(topic, record_fullname, strategy, is_key=False)
    url = f"{sr_url}/subjects/{subject}/versions/latest"
    auth = (sr_user, sr_pass) if (sr_user and sr_pass) else None

    r = _http_get_with_retry(url, auth=auth, timeout=10, max_retries=5)
    schema_json = r.json()
    schema_dict = json.loads(schema_json["schema"])
    clean_logical_types(schema_dict)
    return json.dumps(schema_dict)

# ---------- UDF: bỏ 5 byte (Confluent wire format) ----------
def binary_strip_magic():
    # 1 byte magic (0) + 4 byte schema id (big-endian)
    return udf(lambda b: (b[5:] if b and len(b) > 5 else None), BinaryType())

# ---------- Decode Avro value → cột 'event_time' & flatten ----------
def decode_avro_value(df, schema_str, timestamp_field="timestamp"):
    df2 = df.withColumn("avro_payload", binary_strip_magic()(col("value")))
    decoded = df2.select(from_avro(col("avro_payload"), schema_str).alias("data"))
    flat = decoded.select("data.*")

    if timestamp_field in flat.columns:
        flat = flat.withColumn(
            "event_time",
            from_unixtime((col(timestamp_field).cast("long") / 1000)).cast("timestamp")
        )
    return flat
