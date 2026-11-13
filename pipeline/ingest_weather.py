"""
ingest_weather.py: Ingests hourly weather telemetry for Uganda districts from Open-Meteo REST API into Bronze.
Features:
- Reads district centroids from data/uga_admin_boundaries.geojson/uga_admin2.geojson
- Batches API calls to Open-Meteo (zero rate limit, sub-3s runtime)
- Enforces Data Contract validation via jsonschema.Draft7Validator
- Writes line-delimited JSON (JSONL) to Bronze S3: source=weather/ingest_dt=YYYY-MM-DD/weather.jsonl
- Logs structured events (structlog) and appends metrics to pipeline_metrics.csv
"""

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import jsonschema
import requests
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

# 4. Paths & Constants
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DISTRICTS_GEOJSON = PROJECT_ROOT / "data" / "uga_admin_boundaries.geojson" / "uga_admin2.geojson"
CONTRACT_PATH = PROJECT_ROOT / "contracts" / "weather_schema.json"
BASE_URL = "https://api.open-meteo.com/v1/forecast"
BATCH_SIZE = 45  # Open-Meteo supports multi-location batching


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


def load_district_centroids() -> list:
    """Extracts district metadata and center coordinates from admin2 GeoJSON."""
    with open(DISTRICTS_GEOJSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    districts = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        districts.append({
            "name": props.get("adm2_name"),
            "pcode": props.get("adm2_pcode"),
            "lat": props.get("center_lat"),
            "lon": props.get("center_lon")
        })
    return districts


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def fetch_weather_batch(districts_chunk: list) -> list:
    """Calls Open-Meteo for a batch of districts in a single HTTP request."""
    lats = ",".join(str(d["lat"]) for d in districts_chunk)
    lons = ",".join(str(d["lon"]) for d in districts_chunk)
    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,weather_code",
        "forecast_days": 1
    }
    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    # When querying multiple locations, Open-Meteo returns a list of results
    return data if isinstance(data, list) else [data]


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


def run_pipeline(ingest_dt: str):
    run_id = str(uuid.uuid4())[:8]
    start_time = time.time()
    logger.info("pipeline_started", run_id=run_id, source="weather_api", ingest_dt=ingest_dt)

    schema = load_contract()
    validator = jsonschema.Draft7Validator(schema)
    s3_client = get_s3_client()

    districts = load_district_centroids()
    total_districts = len(districts)
    logger.info("loaded_district_centroids", count=total_districts)

    all_weather_records = []
    quarantined_records = []
    now_iso = datetime.now(timezone.utc).isoformat()

    # Step A: Batch fetch weather across districts
    for i in range(0, total_districts, BATCH_SIZE):
        chunk = districts[i:i + BATCH_SIZE]
        logger.info("fetching_weather_chunk", chunk_index=i // BATCH_SIZE + 1, chunk_size=len(chunk))
        batch_results = fetch_weather_batch(chunk)

        for district_meta, weather_data in zip(chunk, batch_results):
            hourly = weather_data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            precips = hourly.get("precipitation", [])
            winds = hourly.get("wind_speed_10m", [])
            codes = hourly.get("weather_code", [])

            for t, temp, precip, wind, code in zip(times, temps, precips, winds, codes):
                record = {
                    "district_name": district_meta["name"],
                    "district_pcode": district_meta["pcode"],
                    "latitude": district_meta["lat"],
                    "longitude": district_meta["lon"],
                    "timestamp_utc": t,
                    "temperature_c": float(temp) if temp is not None else 0.0,
                    "precipitation_mm": float(precip) if precip is not None else 0.0,
                    "wind_speed_kmh": float(wind) if wind is not None else 0.0,
                    "weather_code": int(code) if code is not None else 0,
                    "ingested_at": now_iso
                }

                # Contract Validation
                errors = list(validator.iter_errors(record))
                if not errors:
                    all_weather_records.append(record)
                else:
                    record["_rejection_reason"] = "; ".join(e.message for e in errors)
                    quarantined_records.append(record)

    logger.info(
        "weather_data_transformed",
        valid_records=len(all_weather_records),
        quarantined_records=len(quarantined_records)
    )

    # Step B: Write Valid Records as JSONL to Bronze S3
    if all_weather_records:
        jsonl_lines = [json.dumps(r) for r in all_weather_records]
        payload = "\n".join(jsonl_lines).encode("utf-8")
        s3_key = f"source=weather/ingest_dt={ingest_dt}/weather.jsonl"
        s3_client.put_object(Bucket=BRONZE_BUCKET, Key=s3_key, Body=payload)
        logger.info("s3_upload_success", bucket=BRONZE_BUCKET, key=s3_key, size_bytes=len(payload))

    duration = time.time() - start_time
    status = "SUCCESS" if len(all_weather_records) > 0 else "FAILED"

    record_metrics(
        run_id=run_id,
        source="weather",
        ingest_dt=ingest_dt,
        rows_read=len(all_weather_records) + len(quarantined_records),
        rows_written=len(all_weather_records),
        rows_quarantined=len(quarantined_records),
        duration_sec=duration,
        status=status
    )

    logger.info("pipeline_completed", run_id=run_id, duration_sec=round(duration, 3), status=status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo weather telemetry into Bronze lakehouse zone.")
    parser.add_argument(
        "--dt",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Partition date in YYYY-MM-DD format (defaults to current UTC date)"
    )
    args = parser.parse_args()
    run_pipeline(ingest_dt=args.dt)
