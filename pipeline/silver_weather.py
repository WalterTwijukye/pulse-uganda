"""
pipeline/silver_weather.py
===========================
Silver Layer Pipeline: Weather Telemetry Parsing, WMO Interpretation,
Pandera Contract Validation, and Partitioned ACID Delta Lake Lakehouse Table.

Architecture & Transformations:
1. Extract Bronze Weather JSONL (s3://de-bronze/source=weather/ingest_dt=YYYY-MM-DD/weather.jsonl)
2. Temporal Standardization:
   - Converts timestamp_utc string ("YYYY-MM-DDTHH:MM") to UTC Datetime (observed_at_ts).
   - Extracts observation_date ("YYYY-MM-DD") for Delta Lake partition pruning.
   - Parses ingested_at to UTC Datetime.
3. Telecom Domain Feature Engineering:
   - Interprets WMO Weather Codes to human-readable weather_condition.
   - Categorizes hydrometeor attenuation risk into rain_severity (NONE, LIGHT, MODERATE, HEAVY).
4. Natural Key Deduplication:
   - Enforces unique (district_pcode, observed_at_ts) per district-hour observation.
5. Pandera Quality Contract Enforcement:
   - Physical checks on atmospheric parameters: temperature (-2 to 48°C), precipitation, wind speed.
   - Geographical envelope checks for Uganda coordinates.
6. Quarantine Routing:
   - Quarantines sensor error anomalies to s3://de-quarantine/source=weather/
7. ACID Lakehouse Persistence:
   - Persists clean records to s3://de-silver/weather/ partitioned by observation_date using Delta-RS.
8. Observability:
   - Emits structured JSON logs and records execution metrics to pipeline_metrics.csv.
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

# Import Pandera quality schema contract
from contracts.silver_weather_contract import silver_weather_schema

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

# WMO Weather Interpretation Table
WMO_INTERPRETATION = {
    0: ("Clear Sky", "NONE"),
    1: ("Mainly Clear", "NONE"),
    2: ("Partly Cloudy", "NONE"),
    3: ("Overcast", "NONE"),
    45: ("Fog", "NONE"),
    48: ("Depositing Rime Fog", "NONE"),
    51: ("Light Drizzle", "LIGHT"),
    53: ("Moderate Drizzle", "LIGHT"),
    55: ("Dense Drizzle", "LIGHT"),
    56: ("Light Freezing Drizzle", "LIGHT"),
    57: ("Dense Freezing Drizzle", "LIGHT"),
    61: ("Slight Rain", "MODERATE"),
    63: ("Moderate Rain", "MODERATE"),
    65: ("Heavy Rain", "HEAVY"),
    66: ("Light Freezing Rain", "MODERATE"),
    67: ("Heavy Freezing Rain", "HEAVY"),
    71: ("Slight Snow", "LIGHT"),
    73: ("Moderate Snow", "MODERATE"),
    75: ("Heavy Snow", "HEAVY"),
    77: ("Snow Grains", "LIGHT"),
    80: ("Slight Rain Showers", "MODERATE"),
    81: ("Moderate Rain Showers", "MODERATE"),
    82: ("Violent Rain Showers", "HEAVY"),
    85: ("Slight Snow Showers", "LIGHT"),
    86: ("Heavy Snow Showers", "HEAVY"),
    95: ("Thunderstorm", "HEAVY"),
    96: ("Thunderstorm with Slight Hail", "HEAVY"),
    99: ("Thunderstorm with Heavy Hail", "HEAVY"),
}


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
# 3. Main Silver Pipeline
# -------------------------------------------------------------------------
def run_silver_weather(dt: str = "2026-10-12", mode: str = "overwrite"):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    s3 = get_s3_client()

    logger.info(
        "silver_weather_started",
        run_id=run_id,
        ingest_dt=dt,
        mode=mode,
    )

    try:
        # Step A: Load Bronze Weather JSONL
        weather_key = f"source=weather/ingest_dt={dt}/weather.jsonl"
        logger.info("loading_bronze_weather", bucket=BRONZE_BUCKET, key=weather_key)
        raw_bytes = fetch_s3_bytes(s3, BRONZE_BUCKET, weather_key)
        df_raw = pl.read_ndjson(io.BytesIO(raw_bytes))
        total_rows_read = len(df_raw)
        logger.info("bronze_weather_loaded", row_count=total_rows_read)

        # Step B: Deduplicate on Natural Key (district_pcode, timestamp_utc)
        df_deduped = df_raw.unique(
            subset=["district_pcode", "timestamp_utc"], keep="last"
        )
        deduped_count = len(df_deduped)
        duplicates_removed = total_rows_read - deduped_count
        if duplicates_removed > 0:
            logger.info(
                "weather_duplicates_removed", count=duplicates_removed
            )

        # Step C: Temporal Standardization
        df_temporal = df_deduped.with_columns(
            pl.col("timestamp_utc")
            .str.to_datetime("%Y-%m-%dT%H:%M")
            .dt.replace_time_zone("UTC")
            .alias("observed_at_ts"),
            pl.col("timestamp_utc").str.slice(0, 10).alias("observation_date"),
            pl.col("ingested_at").str.to_datetime(time_zone="UTC"),
        )

        # Step D: WMO Interpretation & Telecom Rain Severity Engineering
        codes = df_temporal["weather_code"].to_list()
        precips = df_temporal["precipitation_mm"].to_list()

        conditions = []
        severities = []
        for code, precip in zip(codes, precips):
            cond, sev = WMO_INTERPRETATION.get(code, ("Unknown", "NONE"))
            # Hydrometeor precipitation threshold override
            if precip > 15.0:
                sev = "HEAVY"
            elif precip > 5.0 and sev not in ("HEAVY",):
                sev = "MODERATE"
            elif precip > 0.0 and sev == "NONE":
                sev = "LIGHT"
            conditions.append(cond)
            severities.append(sev)

        df_enriched = df_temporal.with_columns(
            pl.Series("weather_condition", conditions, dtype=pl.String),
            pl.Series("rain_severity", severities, dtype=pl.String),
        ).drop("timestamp_utc")

        # Step E: Data Quality Gate Filtering
        # Physics checks: temp [5, 48], precip [0, 300], wind [0, 200], lat [-2, 5], lon [29, 36]
        valid_mask = (
            (pl.col("temperature_c") >= 5.0)
            & (pl.col("temperature_c") <= 48.0)
            & (pl.col("precipitation_mm") >= 0.0)
            & (pl.col("precipitation_mm") <= 300.0)
            & (pl.col("wind_speed_kmh") >= 0.0)
            & (pl.col("wind_speed_kmh") <= 200.0)
            & (pl.col("latitude") >= -2.0)
            & (pl.col("latitude") <= 5.0)
            & (pl.col("longitude") >= 29.0)
            & (pl.col("longitude") <= 36.0)
            & pl.col("district_name").is_not_null()
            & pl.col("district_pcode").is_not_null()
        )

        df_valid = df_enriched.filter(valid_mask)
        df_quarantine = df_enriched.filter(~valid_mask).with_columns(
            pl.lit("physical_range_or_coordinate_anomaly").alias(
                "quarantine_reason"
            ),
            pl.lit(datetime.now(timezone.utc)).alias("quarantined_at"),
        )

        # Step F: Pandera Quality Contract Enforcement
        logger.info(
            "enforcing_pandera_contract",
            valid_candidate_rows=len(df_valid),
            quarantined_rows=len(df_quarantine),
        )
        df_validated = silver_weather_schema.validate(df_valid)
        logger.info(
            "pandera_contract_passed",
            validated_rows=len(df_validated),
        )

        # Step G: Route Quarantined Records to S3
        if len(df_quarantine) > 0:
            quarantine_buf = io.BytesIO()
            df_quarantine.write_parquet(quarantine_buf, compression="snappy")
            quarantine_key = (
                f"source=weather/ingest_dt={dt}/quarantine_{run_id}.parquet"
            )
            upload_s3_bytes(
                s3,
                QUARANTINE_BUCKET,
                quarantine_key,
                quarantine_buf.getvalue(),
            )
            logger.warn(
                "quarantine_records_stored",
                bucket=QUARANTINE_BUCKET,
                key=quarantine_key,
                quarantined_rows=len(df_quarantine),
            )

        # Step H: Persist Clean Telemetry to Delta Lake (Partitioned ACID Table)
        delta_table_uri = f"s3://{SILVER_BUCKET}/weather"
        storage_options = {
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_REGION": "us-east-1",
        }

        logger.info(
            "writing_delta_lake_table",
            uri=delta_table_uri,
            mode=mode,
            partition_by=["observation_date"],
            rows=len(df_validated),
        )
        t0_delta = time.time()
        deltalake.write_deltalake(
            table_or_uri=delta_table_uri,
            data=df_validated.to_arrow(),
            mode=mode,
            partition_by=["observation_date"],
            storage_options=storage_options,
        )
        delta_duration = time.time() - t0_delta

        # Verify Delta Table
        dt_table = deltalake.DeltaTable(
            delta_table_uri, storage_options=storage_options
        )
        logger.info(
            "delta_table_persisted",
            version=dt_table.version(),
            files_count=len(dt_table.file_uris()),
            duration_sec=round(delta_duration, 3),
        )

        total_duration = time.time() - start_time
        record_metrics(
            run_id=run_id,
            source="silver_weather",
            ingest_dt=dt,
            rows_read=total_rows_read,
            rows_written=len(df_validated),
            rows_quarantined=len(df_quarantine),
            duration_sec=total_duration,
            status="SUCCESS",
        )

        logger.info(
            "silver_weather_completed",
            run_id=run_id,
            rows_read=total_rows_read,
            rows_written=len(df_validated),
            rows_quarantined=len(df_quarantine),
            duration_sec=round(total_duration, 3),
            status="SUCCESS",
        )
        return run_id

    except Exception as e:
        total_duration = time.time() - start_time
        logger.error(
            "silver_weather_failed",
            run_id=run_id,
            error=str(e),
            duration_sec=round(total_duration, 3),
        )
        record_metrics(
            run_id=run_id,
            source="silver_weather",
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
        description="Silver Layer Weather Telemetry Pipeline"
    )
    parser.add_argument(
        "--dt",
        type=str,
        default="2026-10-12",
        help="Bronze Weather ingestion date partition (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="overwrite",
        choices=["overwrite", "append"],
        help="Delta Lake write mode",
    )
    args = parser.parse_args()

    run_silver_weather(dt=args.dt, mode=args.mode)
