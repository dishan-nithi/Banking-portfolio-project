import json
import os
import time
import io
from datetime import datetime, timezone
import boto3
import pandas as pd
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "minio-landing-group")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")

TOPICS = {
    "banking.public.customers": "customers",
    "banking.public.accounts": "accounts",
    "banking.public.transactions": "transactions",
}

BATCH_SIZE = 50
FLUSH_INTERVAL_SECONDS = 30

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def ensure_bucket():
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if MINIO_BUCKET not in existing:
        s3.create_bucket(Bucket=MINIO_BUCKET)


def extract_record(event: dict):
    """Turn one Debezium event into one flat record, keeping the operation type.
    Inserts, updates, and snapshot reads use the 'after' image.
    Deletes use the 'before' image, since 'after' is null for a delete."""
    op = event.get("op")
    if op in ("c", "u", "r"):
        row = event.get("after")
    elif op == "d":
        row = event.get("before")
    else:
        return None

    if row is None:
        return None

    record = dict(row)
    record["_op"] = op
    record["_source_ts_ms"] = event.get("source", {}).get("ts_ms")
    record["_lsn"] = event.get("source", {}).get("lsn")
    return record


def write_batch(table, records):
    if not records:
        return
    df = pd.DataFrame(records)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S%f")
    key = f"{table}/dt={date_partition}/{table}_{timestamp}.parquet"

    s3.upload_fileobj(buffer, MINIO_BUCKET, key)
    print(f"Wrote {len(records)} records to s3://{MINIO_BUCKET}/{key}")


def maybe_flush(table, buffers, last_flush, pending_offsets, consumer):
    if not buffers[table]:
        return
    size_ready = len(buffers[table]) >= BATCH_SIZE
    time_ready = (time.monotonic() - last_flush[table]) >= FLUSH_INTERVAL_SECONDS
    if size_ready or time_ready:
        flush_and_commit(table, buffers, pending_offsets, consumer)
        last_flush[table] = time.monotonic()


def flush_and_commit(table, buffers, pending_offsets, consumer):
    if not buffers[table]:
        return
    write_batch(table, buffers[table])
    buffers[table] = []

    if pending_offsets[table]:
        consumer.commit(pending_offsets[table])
        pending_offsets[table] = {}


def main():
    ensure_bucket()

    consumer = KafkaConsumer(
        *TOPICS.keys(),
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        consumer_timeout_ms=1000,
    )

    buffers = {table: [] for table in TOPICS.values()}
    last_flush = {table: time.monotonic() for table in TOPICS.values()}
    pending_offsets = {table: {} for table in TOPICS.values()}

    print("Consumer started, waiting for messages...")

    try:
        while True:
            for message in consumer:
                table = TOPICS.get(message.topic)
                if table is None:
                    continue

                tp = TopicPartition(message.topic, message.partition)
                pending_offsets[table][tp] = OffsetAndMetadata(message.offset + 1, None)

                event = message.value
                if event is not None:
                    record = extract_record(event)
                    if record is not None:
                        buffers[table].append(record)

                maybe_flush(table, buffers, last_flush, pending_offsets, consumer)

            for table in TOPICS.values():
                maybe_flush(table, buffers, last_flush, pending_offsets, consumer)

    except KeyboardInterrupt:
        print("Shutting down, flushing remaining buffers...")
        for table in TOPICS.values():
            flush_and_commit(table, buffers, pending_offsets, consumer)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()