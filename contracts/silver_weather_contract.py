"""
contracts/silver_weather_contract.py
Data quality contract and schema enforcement for Silver Weather using Pandera with Polars engine.
"""

import pandera.polars as pa
import polars as pl

silver_weather_schema = pa.DataFrameSchema(
    columns={
        "district_name": pa.Column(pl.String, nullable=False),
        "district_pcode": pa.Column(pl.String, nullable=False),
        "latitude": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(-2.0),
                pa.Check.less_than_or_equal_to(5.0),
            ],
            nullable=False,
        ),
        "longitude": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(29.0),
                pa.Check.less_than_or_equal_to(36.0),
            ],
            nullable=False,
        ),
        "observed_at_ts": pa.Column(pl.Datetime, nullable=False),
        "observation_date": pa.Column(pl.String, nullable=False),
        "temperature_c": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(5.0),
                pa.Check.less_than_or_equal_to(48.0),
            ],
            nullable=False,
        ),
        "precipitation_mm": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(0.0),
                pa.Check.less_than_or_equal_to(300.0),
            ],
            nullable=False,
        ),
        "wind_speed_kmh": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(0.0),
                pa.Check.less_than_or_equal_to(200.0),
            ],
            nullable=False,
        ),
        "weather_code": pa.Column(
            pl.Int64,
            checks=pa.Check.greater_than_or_equal_to(0),
            nullable=False,
        ),
        "weather_condition": pa.Column(pl.String, nullable=False),
        "rain_severity": pa.Column(
            pl.String,
            checks=pa.Check.isin(["NONE", "LIGHT", "MODERATE", "HEAVY"]),
            nullable=False,
        ),
        "ingested_at": pa.Column(pl.Datetime, nullable=False),
    },
    strict=False,
    coerce=True,
)
