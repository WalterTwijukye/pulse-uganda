"""
contracts/silver_towers_contract.py
Data quality contract and schema enforcement for Silver Towers using Pandera with Polars engine.
"""

import pandera.polars as pa
import polars as pl

silver_towers_schema = pa.DataFrameSchema(
    columns={
        "tower_id": pa.Column(pl.Int64, nullable=False),
        "radio": pa.Column(
            pl.String,
            checks=pa.Check.isin(["GSM", "UMTS", "LTE", "NR"]),
            nullable=False,
        ),
        "mcc": pa.Column(pl.Int64, nullable=False),
        "net": pa.Column(pl.Int64, nullable=False),
        "area": pa.Column(pl.Int64, nullable=False),
        "cell": pa.Column(pl.Int64, nullable=False),
        "district_name": pa.Column(pl.String, nullable=False),
        "district_pcode": pa.Column(pl.String, nullable=False),
        "region_name": pa.Column(pl.String, nullable=False),
        "lon": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(29.0),
                pa.Check.less_than_or_equal_to(36.0),
            ],
            nullable=False,
        ),
        "lat": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(-2.0),
                pa.Check.less_than_or_equal_to(5.0),
            ],
            nullable=False,
        ),
        "range_meters": pa.Column(pl.Int64, nullable=False),
        "created_ts": pa.Column(pl.Datetime, nullable=False),
        "updated_ts": pa.Column(pl.Datetime, nullable=False),
        "ingested_at": pa.Column(pl.Datetime, nullable=False),
    },
    strict=False,
    coerce=True,
)
