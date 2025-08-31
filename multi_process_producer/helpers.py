import json
from pathlib import Path
import logging
from faker import Faker
from pprint import pprint
import time
import random
import os
fake = Faker()


def generate_user_event():
    return {
        "timestamp": int(time.time() * 1000),
        "user_id": random.randint(1000, 9999),
        "session_id": fake.uuid4(),
        "event_type": random.choice(["click", "view", "purchase", "scroll"]),
        "page": fake.uri_path(),
        "referrer": fake.uri(),
        "user_agent": fake.user_agent(),
        "user_process":"bypass-process",
        "route": "bypass"
    }


def get_base_config():
    config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "client.id": os.getenv("CLIENT_ID", "default-client")
    }

    # Nếu SECURITY_PROTOCOL khác PLAINTEXT thì mới thêm SASL
    security_protocol = os.getenv("SECURITY_PROTOCOL", "PLAINTEXT")
    config["security.protocol"] = security_protocol

    if security_protocol != "PLAINTEXT":
        sasl_mechanisms = os.getenv("SASL_MECHANISMS")
        sasl_username = os.getenv("SASL_USERNAME")
        sasl_password = os.getenv("SASL_PASSWORD")

        if not (sasl_mechanisms and sasl_username and sasl_password):
            raise ValueError("Missing SASL configuration in .env file")

        config["sasl.mechanisms"] = sasl_mechanisms
        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    return config

def get_schema_str(file_name="user_event-v1.avsc"):
    pwd = Path(__file__).parent
    schema_file_path = pwd / 'schema' / file_name
    with open(schema_file_path, 'r') as f:
        schemafile = f.read()
        return schemafile

def send_to_dlq(record, topic, producer=None):
    """
    Sends the given record to the specified Dead Letter Queue (DLQ) Kafka topic.

    Parameters:
    - record (dict): The record dictionary to send to DLQ.
    - topic (str): The Kafka topic name for the DLQ.
    - producer (confluent_kafka.Producer): Kafka producer instance used for sending.

    This function JSON-serializes the record and sends it to the DLQ topic.
    """
    try:
        value = json.dumps(record).encode('utf-8')
        producer.produce(
            topic=topic,
            value=value
        )
        producer.flush()
        logging.info(f"Sent record to DLQ topic..")
    except Exception as e:
        logging.error(f"Failed to send record to DLQ: {e}")