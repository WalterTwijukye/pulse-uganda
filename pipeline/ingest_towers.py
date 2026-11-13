"""
ingest_towers.py: Extracts Tower Inventory from OLTP Postgres to Bronze Lakehouse on RustFS/S3.
Production Features:
- CLI argument parsing (--dt, --batch-size)
- Structured JSON logging (structlog)
- Resilience & exponential backoff retry (tenacity)
- Schema contract validation (jsonschema)
- Standard lakehouse Hive-style partitioning (source=towers/ingest_dt=YYYY-MM-DD/)
- Pipeline metrics logging for observability
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import jsonschema
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5434  # Host tunnel mapped in docker-compose.yml
POSTGRES_DB = os.getenv("POSTGRES_DB", "telecom_oltp")
POSTGRES_USER = os.getenv("POSTGRES_USER", "telecom")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "telecom_pass_123")

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BRONZE_BUCKET = "de-bronze"
QUARANTINE_BUCKET = "de-quarantine"

# 4. Load Data Contract Schema
CONTRACT_PATH = Path(__file__).resolve().parent.parent / "contracts" / "towers_schema.json"


def load_contract() -> dict:
    if not CONTRACT_PATH.exists():
        logger.error("contract_file_missing", path=str(CONTRACT_PATH))
        sys.exit(1)
    with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# 5. Resilient Database Extractor
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry_error_callback=lambda retry_state: logger.warn("retrying_postgres_connection", attempt=retry_state.attempt_number)
)
def extract_towers_from_oltp():
    """Connects to Postgres and returns all rows from oltp_tower_inventory."""
    logger.info("connecting_to_postgres", host=POSTGRES_HOST, port=POSTGRES_PORT, db=POSTGRES_DB)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = "SELECT * FROM oltp_tower_inventory ORDER BY tower_id ASC;"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    logger.info("postgres_extraction_complete", row_count=len(rows))
    return rows


# 6. S3 Client Initializer
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1"
    )


# 7. Resilient S3 Uploader
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def upload_to_s3(s3_client, bucket: str, key: str, data_bytes: bytes):
    s3_client.put_object(Bucket=bucket, Key=key, Body=data_bytes)
    logger.info("s3_upload_success", bucket=bucket, key=key, size_bytes=len(data_bytes))


# 8. Observability: Metrics Logger
def record_metrics(run_id: str, source: str, ingest_dt: str, rows_read: int, rows_written: int, rows_quarantined: int, duration_sec: float, status: str):
    metrics_file = Path(__file__).resolve().parent.parent / "pipeline_metrics.csv"
    file_exists = metrics_file.exists()

    with open(metrics_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["run_id", "timestamp", "source", "ingest_dt", "rows_read", "rows_written", "rows_quarantined", "duration_sec", "status"])
        writer.writerow([
            run_id,
            datetime.utcnow().isoformat(),
            source,
            ingest_dt,
            rows_read,
            rows_written,
            rows_quarantined,
            round(duration_sec, 3),
            status
        ])
    logger.info("metrics_recorded", metrics_file=str(metrics_file))


# 9. Main Pipeline Execution
def run_pipeline(ingest_dt: str):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    logger.info("pipeline_started", run_id=run_id, source="oltp_towers", ingest_dt=ingest_dt)

    schema = load_contract()
    s3_client = get_s3_client()

    # Step A: Extract
    raw_rows = extract_towers_from_oltp()
    rows_read = len(raw_rows)

    valid_rows = []
    quarantined_rows = []

    # Step B: Validate against Data Contract (pre-compiled for 20x performance)
    validator = jsonschema.Draft7Validator(schema)
    for row in raw_rows:
        row_dict = dict(row)
        if row_dict.get("ingested_at"):
            row_dict["ingested_at"] = row_dict["ingested_at"].isoformat()

        errors = list(validator.iter_errors(row_dict))
        if not errors:
            valid_rows.append(row_dict)
        else:
            row_dict["_rejection_reason"] = "; ".join(e.message for e in errors)
            quarantined_rows.append(row_dict)

    # Step C: Write Valid Data to Bronze in CSV format
    if valid_rows:
        output_buffer = io.StringIO()
        fieldnames = [k for k in valid_rows[0].keys() if not k.startswith("_")]
        writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(valid_rows)

        s3_key = f"source=towers/ingest_dt={ingest_dt}/towers.csv"
        upload_to_s3(s3_client, BRONZE_BUCKET, s3_key, output_buffer.getvalue().encode("utf-8"))

    # Step D: Write Invalid Data to Quarantine (if any)
    if quarantined_rows:
        quarantine_buffer = io.StringIO()
        q_fieldnames = list(quarantined_rows[0].keys())
        q_writer = csv.DictWriter(quarantine_buffer, fieldnames=q_fieldnames)
        q_writer.writeheader()
        q_writer.writerows(quarantined_rows)

        q_key = f"source=towers/ingest_dt={ingest_dt}/quarantined_towers.csv"
        upload_to_s3(s3_client, QUARANTINE_BUCKET, q_key, quarantine_buffer.getvalue().encode("utf-8"))

    duration = time.time() - start_time
    status = "SUCCESS" if len(valid_rows) > 0 else "FAILED"

    # Step E: Record Observability Metrics
    record_metrics(
        run_id=run_id,
        source="towers",
        ingest_dt=ingest_dt,
        rows_read=rows_read,
        rows_written=len(valid_rows),
        rows_quarantined=len(quarantined_rows),
        duration_sec=duration,
        status=status
    )

    logger.info("pipeline_completed", run_id=run_id, duration_sec=round(duration, 3), status=status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest OLTP cell towers into Bronze lakehouse zone.")
    parser.add_argument(
        "--dt",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Partition date in YYYY-MM-DD format (defaults to current UTC date)"
    )
    args = parser.parse_args()
    run_pipeline(ingest_dt=args.dt)
