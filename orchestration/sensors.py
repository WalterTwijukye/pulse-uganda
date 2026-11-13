"""
orchestration/sensors.py
Event-driven sensors for detecting raw data landings in S3 and triggering automated pipelines.
"""

import json
import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
    define_asset_job,
    AssetSelection,
)

load_dotenv()

# Define targeted jobs for sensors
cdr_downstream_job = define_asset_job(
    "cdr_downstream_job",
    selection=AssetSelection.assets("silver_cdrs")
    | AssetSelection.assets("gold_dbt_models")
    | AssetSelection.assets("gold_s3_export"),
    description="Processes new incoming CDR batches through Silver CDC merge and Gold marts.",
)


@sensor(
    job=cdr_downstream_job,
    minimum_interval_seconds=30,
    description="Monitors s3://de-bronze/source=cdrs/ for new micro-batch Parquet arrivals.",
)
def s3_cdr_sensor(context: SensorEvaluationContext):
    s3_endpoint = "http://localhost:9000"
    s3_access = os.getenv("MINIO_ROOT_USER", "admin")
    s3_secret = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    bucket = "de-bronze"
    prefix = "source=cdrs/"

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access,
        aws_secret_access_key=s3_secret,
    )

    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        context.log.error(f"Failed to query S3: {e}")
        return SkipReason(f"S3 connection error: {e}")

    contents = resp.get("Contents", [])
    parquet_files = sorted(
        [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]
    )

    if not parquet_files:
        return SkipReason("No CDR micro-batch Parquet files found in S3 bronze.")

    # Cursor stores the list of known files as JSON
    known_files = set(json.loads(context.cursor)) if context.cursor else set()
    new_files = [f for f in parquet_files if f not in known_files]

    if not new_files:
        return SkipReason(f"No new CDR batches. All {len(parquet_files)} files already processed.")

    context.log.info(f"Discovered {len(new_files)} new CDR batches: {new_files}")
    new_cursor = json.dumps(parquet_files)

    run_key = f"cdr_batch_{len(parquet_files)}"
    return RunRequest(
        run_key=run_key,
        run_config={},
        cursor=new_cursor,
    )
