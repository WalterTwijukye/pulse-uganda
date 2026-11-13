"""
pipeline/silver_towers.py
==========================
Silver Layer Pipeline: Cell Towers Enrichment, Spatial Point-in-Polygon Join,
Pandera Contract Validation, and ACID Delta Lake Table Persistence.

Architecture & Transformations:
1. Extract Bronze Towers CSV (s3://de-bronze/source=towers/ingest_dt=YYYY-MM-DD/towers.csv)
2. Extract Bronze District GeoJSON (s3://de-bronze/source=geo/ingest_dt=YYYY-MM-DD/uga_admin2.geojson)
3. Spatial STRtree Point-in-Polygon Join:
   - Matches tower (lon, lat) to 135 Uganda district boundaries.
   - Enriches with district_name, district_pcode, and region_name.
4. Temporal Schema Standardization:
   - Converts raw Unix epoch seconds (created, updated) to UTC Datetime (created_ts, updated_ts).
   - Standardizes ingested_at to UTC Datetime.
   - Renames SQL-reserved keyword 'range' to 'range_meters'.
5. Pandera Contract Enforcement:
   - Validates coordinates within Uganda bounding envelope.
   - Restricts radio access technology to ["GSM", "UMTS", "LTE", "NR"].
   - Enforces non-null district assignment on clean records.
6. Quarantine Routing:
   - Out-of-bounds towers routed to s3://de-quarantine/source=towers/ingest_dt=YYYY-MM-DD/
7. ACID Lakehouse Persistence:
   - Writes valid records to s3://de-silver/towers/ using Delta-RS (deltalake).
8. Observability:
   - Emits structured JSON logs and records execution metrics to pipeline_metrics.csv.
"""

import argparse
import io
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
from shapely.geometry import shape, Point
from shapely.strtree import STRtree
import structlog
from dotenv import load_dotenv
import deltalake
from tenacity import retry, stop_after_attempt, wait_exponential

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import the Pandera schema contract
from contracts.silver_towers_contract import silver_towers_schema

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

METRICS_FILE = Path(__file__).resolve().parent.parent / "pipeline_metrics.csv"


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
def run_silver_towers(
    dt: str = "2026-10-07",
    geo_dt: str = "2026-10-12",
    mode: str = "overwrite",
):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    s3 = get_s3_client()

    logger.info(
        "silver_towers_started",
        run_id=run_id,
        ingest_dt=dt,
        geo_dt=geo_dt,
        mode=mode,
    )

    try:
        # Step A: Load Geo Boundaries for Spatial Join
        geo_key = f"source=geo/ingest_dt={geo_dt}/uga_admin2.geojson"
        logger.info("loading_geo_boundaries", bucket=BRONZE_BUCKET, key=geo_key)
        geo_bytes = fetch_s3_bytes(s3, BRONZE_BUCKET, geo_key)
        geo_json = json.loads(geo_bytes.decode("utf-8"))

        district_polygons = []
        district_properties = []
        for feature in geo_json["features"]:
            poly = shape(feature["geometry"])
            district_polygons.append(poly)
            district_properties.append(feature["properties"])

        spatial_tree = STRtree(district_polygons)
        logger.info(
            "spatial_index_constructed",
            num_districts=len(district_polygons),
        )

        # Step B: Load Bronze Towers CSV
        towers_key = f"source=towers/ingest_dt={dt}/towers.csv"
        logger.info("loading_bronze_towers", bucket=BRONZE_BUCKET, key=towers_key)
        towers_bytes = fetch_s3_bytes(s3, BRONZE_BUCKET, towers_key)
        df_raw = pl.read_csv(io.BytesIO(towers_bytes))
        total_rows_read = len(df_raw)
        logger.info("bronze_towers_loaded", row_count=total_rows_read)

        # Step C: Spatial Point-in-Polygon Vectorized Join
        t0_spatial = time.time()
        lons = df_raw["lon"].to_list()
        lats = df_raw["lat"].to_list()
        points = [Point(x, y) for x, y in zip(lons, lats)]

        # Vectorized query: returns 2xN array [point_indices, polygon_indices]
        query_res = spatial_tree.query(points, predicate="intersects")
        pt_indices, poly_indices = query_res[0], query_res[1]

        # Deduplicate border overlaps by taking first matching polygon
        pt_to_poly = {}
        for pt_idx, poly_idx in zip(pt_indices, poly_indices):
            if pt_idx not in pt_to_poly:
                pt_to_poly[pt_idx] = poly_idx

        # Assign matched administrative attributes
        district_names = [None] * total_rows_read
        district_pcodes = [None] * total_rows_read
        region_names = [None] * total_rows_read
        is_matched = [False] * total_rows_read

        for pt_idx, poly_idx in pt_to_poly.items():
            props = district_properties[poly_idx]
            district_names[pt_idx] = props.get("adm2_name")
            district_pcodes[pt_idx] = props.get("adm2_pcode")
            region_names[pt_idx] = props.get("adm1_name")
            is_matched[pt_idx] = True

        df_enriched = df_raw.with_columns(
            pl.Series("district_name", district_names, dtype=pl.String),
            pl.Series("district_pcode", district_pcodes, dtype=pl.String),
            pl.Series("region_name", region_names, dtype=pl.String),
            pl.Series("is_matched", is_matched, dtype=pl.Boolean),
        )

        spatial_duration = time.time() - t0_spatial
        matched_count = sum(is_matched)
        unmatched_count = total_rows_read - matched_count
        logger.info(
            "spatial_join_completed",
            matched_count=matched_count,
            unmatched_count=unmatched_count,
            duration_sec=round(spatial_duration, 3),
        )

        # Step D: Temporal Standardizations & Column Renames
        df_transformed = df_enriched.with_columns(
            pl.from_epoch("created", time_unit="s")
            .dt.replace_time_zone("UTC")
            .alias("created_ts"),
            pl.from_epoch("updated", time_unit="s")
            .dt.replace_time_zone("UTC")
            .alias("updated_ts"),
            pl.col("ingested_at").str.to_datetime(time_zone="UTC"),
        ).rename({"range": "range_meters"})

        # Step E: Split Valid vs Quarantined
        df_valid = df_transformed.filter(pl.col("is_matched")).drop("is_matched")
        df_quarantine = df_transformed.filter(~pl.col("is_matched")).with_columns(
            pl.lit("outside_uganda_district_boundaries").alias("quarantine_reason"),
            pl.lit(datetime.now(timezone.utc)).alias("quarantined_at"),
        )

        # Step F: Pandera Quality Contract Enforcement
        logger.info("enforcing_pandera_contract", valid_candidate_rows=len(df_valid))
        df_validated = silver_towers_schema.validate(df_valid)
        logger.info(
            "pandera_contract_passed",
            validated_rows=len(df_validated),
        )

        # Step G: Route Quarantined Records to S3
        if len(df_quarantine) > 0:
            quarantine_buf = io.BytesIO()
            df_quarantine.write_parquet(quarantine_buf, compression="snappy")
            quarantine_key = (
                f"source=towers/ingest_dt={dt}/quarantine_{run_id}.parquet"
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

        # Step H: Persist Clean Records to Delta Lake (ACID)
        delta_table_uri = f"s3://{SILVER_BUCKET}/towers"
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
            rows=len(df_validated),
        )
        t0_delta = time.time()
        deltalake.write_deltalake(
            table_or_uri=delta_table_uri,
            data=df_validated.to_arrow(),
            mode=mode,
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
            source="silver_towers",
            ingest_dt=dt,
            rows_read=total_rows_read,
            rows_written=len(df_validated),
            rows_quarantined=len(df_quarantine),
            duration_sec=total_duration,
            status="SUCCESS",
        )

        logger.info(
            "silver_towers_completed",
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
            "silver_towers_failed",
            run_id=run_id,
            error=str(e),
            duration_sec=round(total_duration, 3),
        )
        record_metrics(
            run_id=run_id,
            source="silver_towers",
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
        description="Silver Layer Cell Towers Processing Pipeline"
    )
    parser.add_argument(
        "--dt",
        type=str,
        default="2026-10-07",
        help="Bronze Towers ingestion date partition (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--geo-dt",
        type=str,
        default="2026-10-12",
        help="Bronze Geo boundary ingestion date partition (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="overwrite",
        choices=["overwrite", "append"],
        help="Delta Lake write mode",
    )
    args = parser.parse_args()

    run_silver_towers(dt=args.dt, geo_dt=args.geo_dt, mode=args.mode)
