"""
orchestration/definitions.py
Central Dagster Definitions configuration bundling Assets, Jobs, Schedules, and Sensors.
"""

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from orchestration import assets
from orchestration.sensors import cdr_downstream_job, s3_cdr_sensor

# 1. Load all Software-Defined Assets from the assets module
all_assets = load_assets_from_modules([assets])

# 2. Define the Full End-to-End Pipeline Job
telecom_full_pipeline_job = define_asset_job(
    name="telecom_full_pipeline_job",
    selection=AssetSelection.all(),
    description="Full Lakehouse Materialization: Ingests Bronze, transforms Silver ACID Delta tables, builds Gold dbt Star Schema, and exports Parquet marts.",
)

# 3. Define Daily Schedule (Midnight UTC)
daily_telecom_schedule = ScheduleDefinition(
    name="daily_telecom_schedule",
    job=telecom_full_pipeline_job,
    cron_schedule="0 0 * * *",
    description="Nightly run of the complete Uganda Mobile Network Quality pipeline.",
)

# 4. Central Definitions Repository
defs = Definitions(
    assets=all_assets,
    jobs=[telecom_full_pipeline_job, cdr_downstream_job],
    schedules=[daily_telecom_schedule],
    sensors=[s3_cdr_sensor],
)
