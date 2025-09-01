CONSUMER_CONFIG = {
    # Cluster
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),

    # Nhóm & offset
    "group.id": "clickstream-streaming",
    "enable.auto.commit": False,                 # tự commit trong code sau khi xử lý xong
    "auto.offset.reset": "latest",               # backfill thì đổi "earliest"
    "isolation.level": "read_committed",         # khớp enable.idempotence/transactions (nếu có)

    # Rebalance & liveness
    "partition.assignment.strategy": "cooperative-sticky",  # ít gián đoạn
    "session.timeout.ms": 15000,                 # 10–20s
    "heartbeat.interval.ms": 5000,               # ~1/3 session.timeout
    "max.poll.interval.ms": 600000,              # 10 phút nếu xử lý batch lâu

    # Fetch/IO (payload nhỏ, nhiều event)
    "fetch.min.bytes": 1,
    "max.partition.fetch.bytes": 1 * 1024 * 1024,  # 1MB/partition/fetch
    "fetch.max.bytes": 50 * 1024 * 1024,           # 50MB tổng/fetch

    # Hạn chế tạo topic ngoài ý muốn
    "allow.auto.create.topics": False,

    # (Tuỳ môi trường bảo mật)
    # "security.protocol": "SASL_SSL",
    # "sasl.mechanisms": "PLAIN",
    # "sasl.username": os.getenv("SASL_USERNAME"),
    # "sasl.password": os.getenv("SASL_PASSWORD"),
}








from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sr_conf = {
    "url": os.getenv("SCHEMA_REGISTRY_EXTERNAL_URL"),
    "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_USER')}:{os.getenv('SCHEMA_REGISTRY_PASS')}",
}
sr_client = SchemaRegistryClient(sr_conf)

# Dùng subject theo TopicNameStrategy (đúng với producer của bạn)
# Có thể fetch schema id/subject trước, hoặc để AvroDeserializer tự lookup.
value_avro_deserializer = AvroDeserializer(schema_registry_client=sr_client)

consumer_conf = {
    **CONSUMER_CONFIG,
    "key.deserializer": str,                     # nếu key là string
    "value.deserializer": value_avro_deserializer,
    "client.id": "clickstream-consumer-1",
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe([os.getenv("TOPIC_NAME")])






BATCH = 1000
TIMEOUT = 1.0   # giây

while True:
    msgs = consumer.consume(num_messages=BATCH, timeout=TIMEOUT)
    if not msgs:
        continue

    ok = True
    for m in msgs:
        if m.error():
            # ghi log, có thể gửi DLQ
            ok = False
            continue
        record = m.value()  # dict Avro đã deserialize
        # TODO: process(record)

    # commit offset sau khi xử lý thành công batch
    if ok:
        consumer.commit(asynchronous=False)


















spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS")) \
  .option("subscribe", os.getenv("TOPIC_NAME")) \
  .option("startingOffsets", "latest") \
  .option("maxOffsetsPerTrigger", "10000") \
  .option("failOnDataLoss", "false") \
  .load()
"linger.ms": 10,
"batch.size": 15 * 1024,           # 15 KB  (=> khá nhỏ)
"batch.num.messages": 100,          # librdkafka
"queue.buffering.max.kbytes": 100 * 1024,  # = 102,400 KB = ~100 MB (comment đang ghi "100 kb" là sai)
"queue.buffering.max.messages": 850000,
"retries": 3,                       # hơi thấp cho prod
"enable.idempotence": True,
"max.in.flight.requests.per.connection": 5,
"retry.backoff.ms": 500,
"compression.type": "snappy"
PRODUCER_CONFIG = {
    **get_base_config(),

    # Batching
    "linger.ms": 20,                          # gom batch hơn chút
    "batch.size": 128 * 1024,                 # 128KB/partition (nếu dùng Java client style)
    "batch.num.messages": 1000,               # librdkafka: số msg/batch
    "queue.buffering.max.kbytes": 200 * 1024, # ~200MB (tuỳ RAM)
    "queue.buffering.max.messages": 1000000,  # 1M (tuỳ traffic)

    # Độ bền
    "enable.idempotence": True,
    "acks": "all",
    "retries": 2147483647,                    # hoặc số lớn; 3 là hơi ít
    "retry.backoff.ms": 500,
    "max.in.flight.requests.per.connection": 5,

    # Nén
    "compression.type": "lz4",                # nhanh & tỉ lệ nén tốt

    # Khác
    "message.timeout.ms": 240000,             # ~4 phút (tương đương delivery.timeout.ms bên Java)
}
