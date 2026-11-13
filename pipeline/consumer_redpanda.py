"""
consumer_redpanda.py: Consumes streaming CDR telemetry from Redpanda, micro-batches events,
converts them into compressed Apache Parquet, and uploads to Bronze Lakehouse on RustFS/S3.
Implements:
- Manual commit semantics (at-least-once / zero data loss)
- Micro-batching to eliminate the small file problem
- Data contract validation via jsonschema.Draft7Validator
- Checkpointing of offsets
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import jsonschema
from kafka import KafkaConsumer, TopicPartition
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

# 1. Load Environment Configuration
load_dotenv()

# 2. Configure Structured JSON Logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# 3. Connection Parameters
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BRONZE_BUCKET = "de-bronze"

BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "cdr-events"
CONSUMER_GROUP = "telecom-bronze-consumer"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "cdr_schema.json"
CHECKPOINT_DIR = PROJECT_ROOT / "_checkpoints"


def load_contract() -> dict:
    if not CONTRACT_PATH.exists():
        logger.error("contract_file_missing", path=str(CONTRACT_PATH))
        sys.exit(1)
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1"
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def upload_parquet_to_s3(s3_client, bucket: str, key: str, parquet_bytes: bytes):
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_bytes)
    logger.info("parquet_uploaded_to_s3", bucket=bucket, key=key, size_bytes=len(parquet_bytes))


def save_checkpoint(topic: str, partition: int, offset: int):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = CHECKPOINT_DIR / "redpanda_offset.json"
    data = {
        "topic": topic,
        "partition": partition,
        "last_committed_offset": offset,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def record_metrics(run_id: str, source: str, ingest_dt: str, rows_read: int, rows_written: int, rows_quarantined: int, duration_sec: float, status: str):
    metrics_file = PROJECT_ROOT / "pipeline_metrics.csv"
    file_exists = metrics_file.exists()

    with open(metrics_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["run_id", "timestamp", "source", "ingest_dt", "rows_read", "rows_written", "rows_quarantined", "duration_sec", "status"])
        writer.writerow([
            run_id,
            datetime.now(timezone.utc).isoformat(),
            source,
            ingest_dt,
            rows_read,
            rows_written,
            rows_quarantined,
            round(duration_sec, 3),
            status
        ])


def flush_batch_to_parquet(s3_client, records: list, ingest_dt: str) -> str:
    """Converts a micro-batch of dict records to Parquet and uploads to Bronze S3."""
    batch_id = str(uuid.uuid4())[:8]
    table = pa.Table.from_pylist(records)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="SNAPPY")
    parquet_bytes = buf.getvalue()

    s3_key = f"source=cdrs/ingest_dt={ingest_dt}/cdrs_{batch_id}.parquet"
    upload_parquet_to_s3(s3_client, BRONZE_BUCKET, s3_key, parquet_bytes)
    return s3_key


def consume_stream(topic: str, batch_size: int, ingest_dt: str, max_records: int):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    logger.info("consumer_started", run_id=run_id, topic=topic, batch_size=batch_size, ingest_dt=ingest_dt)

    schema = load_contract()
    validator = jsonschema.Draft7Validator(schema)
    s3_client = get_s3_client()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual commit for exactly-once landing
        consumer_timeout_ms=5000,   # Stops after 5s of inactivity when queue drained
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    batch = []
    total_consumed = 0
    total_written = 0
    total_quarantined = 0
    batches_uploaded = 0

    try:
        for message in consumer:
            record = message.value
            total_consumed += 1

            # Validate against contract
            errors = list(validator.iter_errors(record))
            if not errors:
                batch.append(record)
            else:
                total_quarantined += 1
                logger.warn("cdr_contract_violation", offset=message.offset, error=errors[0].message)

            # Flush condition: Batch size reached
            if len(batch) >= batch_size:
                flush_batch_to_parquet(s3_client, batch, ingest_dt)
                total_written += len(batch)
                batches_uploaded += 1

                # Commit Kafka offset after successful write
                consumer.commit()
                save_checkpoint(topic, message.partition, message.offset)
                logger.info("batch_committed", batch_count=len(batch), offset=message.offset)
                batch.clear()

            if max_records > 0 and total_consumed >= max_records:
                logger.info("max_records_reached", total=total_consumed)
                break

        # Flush any remaining tail records in the buffer
        if batch:
            flush_batch_to_parquet(s3_client, batch, ingest_dt)
            total_written += len(batch)
            batches_uploaded += 1
            consumer.commit()
            save_checkpoint(topic, message.partition, message.offset)
            logger.info("tail_batch_committed", batch_count=len(batch), offset=message.offset)
            batch.clear()

    finally:
        consumer.close()

    duration = time.time() - start_time
    status = "SUCCESS" if total_written > 0 else "NO_DATA"

    record_metrics(
        run_id=run_id,
        source="cdrs",
        ingest_dt=ingest_dt,
        rows_read=total_consumed,
        rows_written=total_written,
        rows_quarantined=total_quarantined,
        duration_sec=duration,
        status=status
    )

    logger.info(
        "consumer_completed",
        run_id=run_id,
        total_consumed=total_consumed,
        total_written=total_written,
        batches_uploaded=batches_uploaded,
        duration_sec=round(duration, 3),
        status=status
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume CDRs from Redpanda into Bronze Parquet.")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka topic to consume")
    parser.add_argument("--batch-size", type=int, default=1000, help="Micro-batch size for Parquet files")
    parser.add_argument(
        "--dt",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Partition date in YYYY-MM-DD format"
    )
    parser.add_argument("--max-records", type=int, default=0, help="Max records to consume (0 for all available)")
    args = parser.parse_args()

    consume_stream(topic=args.topic, batch_size=args.batch_size, ingest_dt=args.dt, max_records=args.max_records)
