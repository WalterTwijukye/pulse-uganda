"""
pipeline/export_gold.py
========================
Gold Layer Export Pipeline: Exports dbt-transformed dimensional models and
analytics marts from DuckDB to Gold S3 Storage (s3://de-gold/).
"""

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import structlog
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
GOLD_BUCKET = "de-gold"
DBT_DB_PATH = PROJECT_ROOT / "gold" / "gold.duckdb"
METRICS_FILE = PROJECT_ROOT / "pipeline_metrics.csv"

GOLD_TABLES = [
    "dim_towers",
    "dim_weather",
    "dim_date",
    "fct_cdr_events",
    "agg_network_quality_daily",
]


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


def export_gold_tables():
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()

    logger.info("gold_export_started", run_id=run_id, bucket=GOLD_BUCKET)

    if not DBT_DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found at {DBT_DB_PATH}")

    con = duckdb.connect(str(DBT_DB_PATH))
    con.execute("LOAD httpfs;")

    # Configure S3 secret for export
    con.execute(f"""
    CREATE SECRET IF NOT EXISTS s3_export_secret (
        TYPE S3,
        KEY_ID '{S3_ACCESS_KEY}',
        SECRET '{S3_SECRET_KEY}',
        ENDPOINT 'localhost:9000',
        USE_SSL false,
        URL_STYLE 'path'
    );
    """)

    total_rows_exported = 0

    for table in GOLD_TABLES:
        t0 = time.time()
        row_count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        s3_target = f"s3://{GOLD_BUCKET}/{table}/{table}.parquet"

        logger.info(
            "exporting_table_to_s3",
            table=table,
            rows=row_count,
            target=s3_target,
        )

        con.execute(f"""
        COPY (SELECT * FROM {table}) TO '{s3_target}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """)

        duration = time.time() - t0
        total_rows_exported += row_count
        logger.info(
            "table_exported",
            table=table,
            rows=row_count,
            duration_sec=round(duration, 3),
        )

    # Fetch executive sample from agg_network_quality_daily
    qos_summary = con.execute("""
    SELECT 
        district_name,
        operator_name,
        total_calls,
        dropped_calls,
        drop_call_rate_pct,
        meets_ucc_qos_standard,
        rain_affected_calls
    FROM agg_network_quality_daily
    ORDER BY total_calls DESC
    LIMIT 5;
    """).df()

    total_duration = time.time() - start_time
    record_metrics(
        run_id=run_id,
        source="gold_export",
        ingest_dt=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        rows_read=total_rows_exported,
        rows_written=total_rows_exported,
        rows_quarantined=0,
        duration_sec=total_duration,
        status="SUCCESS",
    )

    logger.info(
        "gold_export_completed",
        run_id=run_id,
        tables_exported=len(GOLD_TABLES),
        total_rows=total_rows_exported,
        duration_sec=round(total_duration, 3),
        status="SUCCESS",
    )

    print("\n================= TOP 5 DISTRICTS QOS METRICS =================")
    print(qos_summary.to_string(index=False))
    print("================================================================\n")

    return run_id


if __name__ == "__main__":
    export_gold_tables()
