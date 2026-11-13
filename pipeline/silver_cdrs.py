"""
pipeline/silver_cdrs.py
========================
Silver Layer Pipeline: Streaming Call Detail Records (CDRs) Transformation,
Pandera Contract Validation, and Change Data Capture (CDC) ACID MERGE into Delta Lake.

Architecture & Transformations:
1. Discover Bronze CDR Parquet Micro-Batches:
   - Scans s3://de-bronze/source=cdrs/ingest_dt=YYYY-MM-DD/
   - Processes batches sequentially in arrival order.
2. Temporal & Telecom Feature Engineering:
   - Standardizes timestamp string to UTC Datetime (call_ts).
   - Extracts call_date ("YYYY-MM-DD") for temporal analytics.
   - Computes is_dropped_call boolean flag (termination_code != 'NORMAL').
   - Categorizes radio frequency signal quality (rsrp_dbm) into EXCELLENT, GOOD, FAIR, POOR.
   - Records updated_at_ts audit timestamp.
3. Quality Gate Enforcement (Pandera):
   - Validates MSISDN regex (+256...), tower_id range (1..33051), cellular RF ranges.
   - Isolates non-compliant records to s3://de-quarantine/source=cdrs/
4. Intra-Batch CDC State Resolution:
   - Resolves multiple state transitions (INSERT -> UPDATE -> DELETE) within the same batch.
   - Collapses duplicates to ensure unique primary key (call_id) per merge transaction.
5. Delta Lake ACID MERGE Engine:
   - Executes MERGE INTO target using Delta-RS:
     * WHEN MATCHED AND source.op = 'DELETE' THEN DELETE
     * WHEN MATCHED AND source.op = 'UPDATE' THEN UPDATE SET *
     * WHEN NOT MATCHED AND source.op = 'INSERT' THEN INSERT *
6. Observability:
   - Emits structured JSON logs per micro-batch transaction.
   - Records cumulative run metrics in pipeline_metrics.csv.
"""

import argparse
import io
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
import structlog
from dotenv import load_dotenv
import deltalake
from tenacity import retry, stop_after_attempt, wait_exponential

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import Pandera schema contract
from contracts.silver_cdr_contract import silver_cdr_schema

# -------------------------------------------------------------------------
# 1. Environment & Logging Configuration
# -------------------------------------------------------------------------
load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BRONZE_BUCKET = "de-bronze"
SILVER_BUCKET = "de-silver"
QUARANTINE_BUCKET = "de-quarantine"

METRICS_FILE = PROJECT_ROOT / "pipeline_metrics.csv"


# -------------------------------------------------------------------------
# 2. Resilient S3 Helper
# -------------------------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_s3_bytes(s3_client, bucket: str, key: str) -> bytes:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def upload_s3_bytes(s3_client, bucket: str, key: str, data: bytes):
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)


def record_metrics(
    run_id: str,
    source: str,
    ingest_dt: str,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
    duration_sec: float,
    status: str,
):
    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_exists = METRICS_FILE.exists()
    ts_now = datetime.now(timezone.utc).isoformat()
    with open(METRICS_FILE, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write(
                "run_id,timestamp,source,ingest_dt,rows_read,rows_written,rows_quarantined,duration_sec,status\n"
            )
        f.write(
            f"{run_id},{ts_now},{source},{ingest_dt},{rows_read},{rows_written},{rows_quarantined},{duration_sec:.3f},{status}\n"
        )


# -------------------------------------------------------------------------
# 3. CDC Intra-Batch State Resolution
# -------------------------------------------------------------------------
def resolve_intra_batch_cdc(df: pl.DataFrame) -> pl.DataFrame:
    """
    Collapses multiple events for the same call_id within a single micro-batch.
    Ensures that the source table passed to Delta MERGE has unique primary keys.

    Rules:
    - If first op is 'INSERT' and last op is 'DELETE', call was born and died
      within this batch; exclude from target write.
    - If first op is 'INSERT' and last op is 'UPDATE', effective op is 'INSERT'
      with the latest updated metrics.
    - If first op is 'UPDATE' or 'DELETE', effective op is the last op.
    """
    if len(df) == df["call_id"].n_unique():
        return df

    df_sorted = df.sort(["call_id", "call_ts"])
    first_ops = (
        df_sorted.group_by("call_id")
        .first()
        .select(["call_id", pl.col("op").alias("first_op")])
    )
    last_records = df_sorted.group_by("call_id").last()

    merged = last_records.join(first_ops, on="call_id")

    effective_ops = []
    for first_op, last_op in zip(merged["first_op"], merged["op"]):
        if first_op == "INSERT":
            effective_ops.append(
                "INSERT" if last_op != "DELETE" else "TRANSIENT_DELETE"
            )
        else:
            effective_ops.append(last_op)

    resolved = (
        merged.with_columns(pl.Series("effective_op", effective_ops))
        .drop(["first_op", "op"])
        .rename({"effective_op": "op"})
        .filter(pl.col("op") != "TRANSIENT_DELETE")
    )
    return resolved


# -------------------------------------------------------------------------
# 4. Target Schema Initialization Helper
# -------------------------------------------------------------------------
def get_target_empty_dataframe() -> pl.DataFrame:
    return pl.DataFrame({
        "call_id": pl.Series([], dtype=pl.String),
        "caller_msisdn": pl.Series([], dtype=pl.String),
        "callee_msisdn": pl.Series([], dtype=pl.String),
        "tower_id": pl.Series([], dtype=pl.Int64),
        "event_type": pl.Series([], dtype=pl.String),
        "call_ts": pl.Series([], dtype=pl.Datetime(time_unit="us", time_zone="UTC")),
        "call_date": pl.Series([], dtype=pl.String),
        "duration_sec": pl.Series([], dtype=pl.Int64),
        "termination_code": pl.Series([], dtype=pl.String),
        "is_dropped_call": pl.Series([], dtype=pl.Boolean),
        "rsrp_dbm": pl.Series([], dtype=pl.Float64),
        "rsrq_db": pl.Series([], dtype=pl.Float64),
        "signal_quality": pl.Series([], dtype=pl.String),
        "latency_ms": pl.Series([], dtype=pl.Int64),
        "op": pl.Series([], dtype=pl.String),
        "updated_at_ts": pl.Series([], dtype=pl.Datetime(time_unit="us", time_zone="UTC")),
    })


# -------------------------------------------------------------------------
# 5. Main Silver Pipeline
# -------------------------------------------------------------------------
def run_silver_cdrs(dt: str = "2026-10-14"):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    s3 = get_s3_client()

    logger.info(
        "silver_cdrs_started",
        run_id=run_id,
        ingest_dt=dt,
    )

    try:
        # Step A: Discover Bronze Micro-Batches
        prefix = f"source=cdrs/ingest_dt={dt}/"
        resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
        contents = resp.get("Contents", [])
        parquet_keys = sorted(
            [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]
        )

        if not parquet_keys:
            raise FileNotFoundError(
                f"No Bronze CDR Parquet files found in s3://{BRONZE_BUCKET}/{prefix}"
            )

        logger.info(
            "bronze_batches_discovered",
            batch_count=len(parquet_keys),
            keys=[k.split("/")[-1] for k in parquet_keys],
        )

        storage_options = {
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_REGION": "us-east-1",
        }
        delta_table_uri = f"s3://{SILVER_BUCKET}/cdrs"

        # Step B: Ensure Target Delta Table Exists
        table_exists = deltalake.DeltaTable.is_deltatable(
            delta_table_uri, storage_options=storage_options
        )
        if not table_exists:
            logger.info("initializing_silver_cdrs_delta_table", uri=delta_table_uri)
            empty_df = get_target_empty_dataframe()
            deltalake.write_deltalake(
                table_or_uri=delta_table_uri,
                data=empty_df.to_arrow(),
                mode="overwrite",
                storage_options=storage_options,
            )

        total_rows_read = 0
        total_quarantined = 0
        total_inserted = 0
        total_updated = 0
        total_deleted = 0

        # Step C: Sequentially Process & Merge Each Micro-Batch
        for batch_idx, s3_key in enumerate(parquet_keys, start=1):
            file_name = s3_key.split("/")[-1]
            logger.info(
                "processing_micro_batch",
                batch_index=batch_idx,
                file=file_name,
            )

            raw_bytes = fetch_s3_bytes(s3, BRONZE_BUCKET, s3_key)
            df_batch = pl.read_parquet(io.BytesIO(raw_bytes))
            batch_rows = len(df_batch)
            total_rows_read += batch_rows

            # 1. Feature Engineering & Transformations
            now_utc = datetime.now(timezone.utc)
            df_transformed = df_batch.with_columns(
                pl.col("timestamp")
                .str.to_datetime(time_zone="UTC")
                .alias("call_ts"),
                pl.col("timestamp").str.slice(0, 10).alias("call_date"),
                (pl.col("termination_code") != "NORMAL").alias("is_dropped_call"),
                pl.when(pl.col("rsrp_dbm") >= -80.0)
                .then(pl.lit("EXCELLENT"))
                .when(pl.col("rsrp_dbm") >= -90.0)
                .then(pl.lit("GOOD"))
                .when(pl.col("rsrp_dbm") >= -100.0)
                .then(pl.lit("FAIR"))
                .otherwise(pl.lit("POOR"))
                .alias("signal_quality"),
                pl.lit(now_utc).alias("updated_at_ts"),
            ).drop("timestamp")

            # 2. Quality Gate & Quarantine Filtering
            valid_mask = (
                pl.col("caller_msisdn").str.contains(r"^\+256[0-9]{9}$")
                & pl.col("callee_msisdn").str.contains(r"^\+256[0-9]{9}$")
                & (pl.col("tower_id") >= 1)
                & (pl.col("tower_id") <= 33051)
                & pl.col("event_type").is_in(["VOICE", "SMS", "DATA"])
                & (pl.col("duration_sec") >= 0)
                & (pl.col("duration_sec") <= 14400)
                & (pl.col("rsrp_dbm") >= -140.0)
                & (pl.col("rsrp_dbm") <= -40.0)
                & (pl.col("rsrq_db") >= -25.0)
                & (pl.col("rsrq_db") <= 0.0)
                & (pl.col("latency_ms") >= 1)
                & (pl.col("latency_ms") <= 2000)
                & pl.col("op").is_in(["INSERT", "UPDATE", "DELETE"])
            )

            df_valid = df_transformed.filter(valid_mask)
            df_quarantine = df_transformed.filter(~valid_mask)

            if len(df_quarantine) > 0:
                quarantine_buf = io.BytesIO()
                df_quarantine.write_parquet(quarantine_buf, compression="snappy")
                quarantine_key = f"source=cdrs/ingest_dt={dt}/quarantine_{run_id}_batch_{batch_idx}.parquet"
                upload_s3_bytes(
                    s3,
                    QUARANTINE_BUCKET,
                    quarantine_key,
                    quarantine_buf.getvalue(),
                )
                total_quarantined += len(df_quarantine)
                logger.warn(
                    "quarantine_records_stored",
                    file=file_name,
                    quarantined=len(df_quarantine),
                )

            # 3. Pandera Schema Validation
            df_validated = silver_cdr_schema.validate(df_valid)

            # 4. Intra-Batch CDC Resolution
            df_resolved = resolve_intra_batch_cdc(df_validated)
            logger.info(
                "cdc_intra_batch_resolved",
                raw_rows=len(df_validated),
                resolved_unique_keys=len(df_resolved),
            )

            # 5. Delta Lake ACID MERGE
            dt_table = deltalake.DeltaTable(
                delta_table_uri, storage_options=storage_options
            )
            t0_merge = time.time()
            merge_metrics = (
                dt_table.merge(
                    source=df_resolved.to_arrow(),
                    predicate="target.call_id = source.call_id",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_delete(predicate="source.op = 'DELETE'")
                .when_matched_update_all(predicate="source.op = 'UPDATE'")
                .when_not_matched_insert_all(predicate="source.op = 'INSERT'")
                .execute()
            )
            merge_duration = time.time() - t0_merge

            inserted = merge_metrics.get("num_target_rows_inserted", 0)
            updated = merge_metrics.get("num_target_rows_updated", 0)
            deleted = merge_metrics.get("num_target_rows_deleted", 0)
            total_inserted += inserted
            total_updated += updated
            total_deleted += deleted

            logger.info(
                "batch_merge_committed",
                batch_index=batch_idx,
                inserted=inserted,
                updated=updated,
                deleted=deleted,
                duration_sec=round(merge_duration, 3),
            )

        # Step D: Final Delta Table Audit
        dt_final = deltalake.DeltaTable(
            delta_table_uri, storage_options=storage_options
        )
        final_arrow = dt_final.to_pyarrow_table()
        df_final = pl.from_arrow(final_arrow)
        current_rows = len(df_final)

        total_duration = time.time() - start_time
        record_metrics(
            run_id=run_id,
            source="silver_cdrs",
            ingest_dt=dt,
            rows_read=total_rows_read,
            rows_written=current_rows,
            rows_quarantined=total_quarantined,
            duration_sec=total_duration,
            status="SUCCESS",
        )

        logger.info(
            "silver_cdrs_completed",
            run_id=run_id,
            total_rows_read=total_rows_read,
            final_active_rows=current_rows,
            total_inserted=total_inserted,
            total_updated=total_updated,
            total_deleted=total_deleted,
            table_version=dt_final.version(),
            files_count=len(dt_final.file_uris()),
            duration_sec=round(total_duration, 3),
            status="SUCCESS",
        )
        return run_id

    except Exception as e:
        total_duration = time.time() - start_time
        logger.error(
            "silver_cdrs_failed",
            run_id=run_id,
            error=str(e),
            duration_sec=round(total_duration, 3),
        )
        record_metrics(
            run_id=run_id,
            source="silver_cdrs",
            ingest_dt=dt,
            rows_read=total_rows_read if "total_rows_read" in locals() else 0,
            rows_written=0,
            rows_quarantined=0,
            duration_sec=total_duration,
            status="FAILED",
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver Layer Streaming CDRs CDC Merge Pipeline"
    )
    parser.add_argument(
        "--dt",
        type=str,
        default="2026-10-14",
        help="Bronze CDR ingestion date partition (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    run_silver_cdrs(dt=args.dt)
