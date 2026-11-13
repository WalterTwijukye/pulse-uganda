"""
ingest_geo.py: Ingests Uganda Administrative Boundary GeoJSON Layers into Bronze Lakehouse on RustFS/S3.
Ingests admin levels 0 (National), 1 (Regions), 2 (Districts), 3 (Counties), 4 (Parishes).
"""

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import jsonschema
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

# 4. Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GEO_DATA_DIR = PROJECT_ROOT / "data" / "uga_admin_boundaries.geojson"
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "geo_schema.json"

TARGET_LAYERS = [
    "uga_admin0.geojson",  # National
    "uga_admin1.geojson",  # Regions (4)
    "uga_admin2.geojson",  # Districts (135)
    "uga_admin3.geojson",  # Counties (208)
    "uga_admin4.geojson",  # Parishes (1,520)
]


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
def upload_file_to_s3(s3_client, bucket: str, key: str, file_path: Path):
    with open(file_path, "rb") as f:
        data = f.read()
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("s3_upload_success", bucket=bucket, key=key, size_bytes=len(data))
    return len(data)


def record_metrics(run_id: str, source: str, ingest_dt: str, rows_read: int, rows_written: int, rows_quarantined: int, duration_sec: float, status: str):
    metrics_file = PROJECT_ROOT / "pipeline_metrics.csv"
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


def run_pipeline(ingest_dt: str):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    logger.info("pipeline_started", run_id=run_id, source="geo_reference", ingest_dt=ingest_dt)

    schema = load_contract()
    validator = jsonschema.Draft7Validator(schema)
    s3_client = get_s3_client()

    total_layers_processed = 0
    total_features_ingested = 0

    for layer_filename in TARGET_LAYERS:
        file_path = GEO_DATA_DIR / layer_filename
        if not file_path.exists():
            logger.error("layer_file_missing", file=layer_filename)
            continue

        logger.info("processing_layer", layer=layer_filename)
        with open(file_path, "r", encoding="utf-8") as f:
            geojson_data = json.load(f)

        # Validate against contract
        errors = list(validator.iter_errors(geojson_data))
        if errors:
            logger.error("schema_validation_failed", layer=layer_filename, error=errors[0].message)
            continue

        feature_count = len(geojson_data.get("features", []))
        total_features_ingested += feature_count

        # Upload to Bronze
        s3_key = f"source=geo/ingest_dt={ingest_dt}/{layer_filename}"
        upload_file_to_s3(s3_client, BRONZE_BUCKET, s3_key, file_path)
        total_layers_processed += 1

    duration = time.time() - start_time
    status = "SUCCESS" if total_layers_processed == len(TARGET_LAYERS) else "PARTIAL_SUCCESS"

    record_metrics(
        run_id=run_id,
        source="geo",
        ingest_dt=ingest_dt,
        rows_read=total_features_ingested,
        rows_written=total_features_ingested,
        rows_quarantined=0,
        duration_sec=duration,
        status=status
    )

    logger.info(
        "pipeline_completed",
        run_id=run_id,
        layers_uploaded=total_layers_processed,
        total_features=total_features_ingested,
        duration_sec=round(duration, 3),
        status=status
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Uganda spatial boundaries into Bronze lakehouse zone.")
    parser.add_argument(
        "--dt",
        type=str,
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Partition date in YYYY-MM-DD format (defaults to current UTC date)"
    )
    args = parser.parse_args()
    run_pipeline(ingest_dt=args.dt)
