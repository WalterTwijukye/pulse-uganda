"""
tests/test_hardening_scenarios.py
Automated Verification Suite for Week 4 Hardening & Production Reliability:
1. Scenario 1: Idempotency (Silver pipeline re-execution preserves exact state with zero duplicate drift).
2. Scenario 2: Delta Lake Time Travel (Point-in-time state queryability across commits).
3. Scenario 3: Quarantine Isolation (Out-of-bounds towers & bad records routed to quarantine).
4. Scenario 4: Gold Star Schema Referential Integrity (100% fact join rate with Kimball unknown member).
5. Scenario 5: Dagster Orchestration Integrity (Asset graph, sensors, and jobs).
"""

import os
import sys
from pathlib import Path
import pytest
import deltalake
import duckdb
from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "admin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "password123"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
}


def test_scenario_1_silver_weather_idempotency():
    """
    Scenario 1: Idempotency Verification.
    Re-running silver weather on partition 2026-10-12 must produce the EXACT
    same active row count (3,240 rows) without appending duplicates.
    """
    from pipeline.silver_weather import run_silver_weather

    uri = "s3://de-silver/weather"
    dt_before = deltalake.DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    rows_before = dt_before.to_pyarrow_table().num_rows

    # Re-run pipeline in overwrite mode
    run_silver_weather(dt="2026-10-12", mode="overwrite")

    dt_after = deltalake.DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    rows_after = dt_after.to_pyarrow_table().num_rows

    assert rows_before == 3240, f"Expected 3,240 rows before run, got {rows_before}"
    assert rows_after == 3240, f"Idempotency violated: row count changed to {rows_after}"
    assert rows_before == rows_after, "Re-running pipeline produced row count drift!"


def test_scenario_2_delta_lake_time_travel():
    """
    Scenario 2: Delta Lake Time Travel Verification.
    Delta Lake must retain previous table states, allowing point-in-time auditing
    without restoring backups or modifying disk state.
    """
    uri = "s3://de-silver/cdrs"
    dt = deltalake.DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    current_version = dt.version()
    assert current_version >= 3, f"Expected at least 3 committed versions, found {current_version}"

    # Read Version 1
    dt_v1 = deltalake.DeltaTable(uri, version=1, storage_options=STORAGE_OPTIONS)
    v1_rows = dt_v1.to_pyarrow_table().num_rows
    assert 900 <= v1_rows <= 1000, f"Version 1 row count unexpected: {v1_rows}"

    # Read Version 2
    dt_v2 = deltalake.DeltaTable(uri, version=2, storage_options=STORAGE_OPTIONS)
    v2_rows = dt_v2.to_pyarrow_table().num_rows
    assert 1800 <= v2_rows <= 2000, f"Version 2 row count unexpected: {v2_rows}"

    # Read Latest Version
    dt_latest = deltalake.DeltaTable(uri, storage_options=STORAGE_OPTIONS)
    latest_rows = dt_latest.to_pyarrow_table().num_rows
    assert latest_rows == 2330, f"Latest version should have 2,330 active rows, found {latest_rows}"

    # Verify that earlier version has fewer records than latest
    assert v1_rows < v2_rows < latest_rows, "Delta Lake Time Travel progression violated!"


def test_scenario_3_quarantine_isolation():
    """
    Scenario 3: Data Contract Quarantine Routing.
    Verify that contract-violating records (out-of-bounds towers outside Uganda)
    were quarantined and NOT merged into the Silver table.
    """
    uri_silver = "s3://de-silver/towers"
    dt_silver = deltalake.DeltaTable(uri_silver, storage_options=STORAGE_OPTIONS)
    arrow_silver = dt_silver.to_pyarrow_table()

    # Active Silver towers must be matched to a district
    district_names = arrow_silver.column("district_name").to_pylist()
    assert all(d is not None for d in district_names), "Found null district in Silver towers!"

    # Row count check: 32,655 clean towers in Silver
    assert arrow_silver.num_rows == 32655, f"Expected 32,655 clean towers, found {arrow_silver.num_rows}"


def test_scenario_4_star_schema_referential_integrity():
    """
    Scenario 4: Gold Star Schema Referential Integrity.
    DuckDB fact records must join to dim_towers with 100% integrity
    via the Kimball Unknown Member (-1) pattern.
    """
    db_path = PROJECT_ROOT / "gold" / "gold.duckdb"
    assert db_path.exists(), f"DuckDB database not found at {db_path}"

    con = duckdb.connect(str(db_path), read_only=True)

    # 1. Check Unknown Member exists
    unknown_member = con.execute("SELECT count(*) FROM dim_towers WHERE tower_id = -1").fetchone()[0]
    assert unknown_member == 1, "Kimball Unknown Member (-1) missing from dim_towers!"

    # 2. Check Fact Foreign Key Referential Integrity
    orphaned_towers = con.execute("""
        SELECT count(*)
        FROM fct_cdr_events f
        LEFT JOIN dim_towers d ON f.tower_id = d.tower_id
        WHERE d.tower_id IS NULL;
    """).fetchone()[0]
    assert orphaned_towers == 0, f"Referential integrity failure: {orphaned_towers} orphaned calls!"

    # 3. Check Total Fact Call Events Count
    total_calls = con.execute("SELECT count(*) FROM fct_cdr_events").fetchone()[0]
    assert total_calls == 2330, f"Expected 2,330 calls in fact table, found {total_calls}"


def test_scenario_5_dagster_orchestration_definitions():
    """
    Scenario 5: Dagster Definitions & Lineage Graph Integrity.
    Assert all 9 Software-Defined Assets, jobs, and sensors are valid.
    """
    from orchestration.definitions import defs

    asset_keys = {a.key.path[0] for a in defs.assets}
    expected_assets = {
        "bronze_towers", "bronze_geo", "bronze_weather", "bronze_cdrs",
        "silver_towers", "silver_weather", "silver_cdrs",
        "gold_dbt_models", "gold_s3_export"
    }
    assert expected_assets.issubset(asset_keys), f"Missing assets in Dagster definitions: {expected_assets - asset_keys}"

    # Verify sensor presence
    sensor_names = {s.name for s in defs.sensors}
    assert "s3_cdr_sensor" in sensor_names, "s3_cdr_sensor missing from Dagster definitions!"

    # Verify job presence
    job_names = {j.name for j in defs.jobs}
    assert "telecom_full_pipeline_job" in job_names, "telecom_full_pipeline_job missing from Dagster definitions!"
    assert "cdr_downstream_job" in job_names, "cdr_downstream_job missing from Dagster definitions!"

