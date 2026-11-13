"""
contracts/silver_cdr_contract.py
Data quality contract and schema enforcement for Silver CDRs using Pandera with Polars engine.
"""

import pandera.polars as pa
import polars as pl

silver_cdr_schema = pa.DataFrameSchema(
    columns={
        "call_id": pa.Column(pl.String, nullable=False),
        "caller_msisdn": pa.Column(
            pl.String,
            checks=pa.Check.str_matches(r"^\+256[0-9]{9}$"),
            nullable=False,
        ),
        "callee_msisdn": pa.Column(
            pl.String,
            checks=pa.Check.str_matches(r"^\+256[0-9]{9}$"),
            nullable=False,
        ),
        "tower_id": pa.Column(
            pl.Int64,
            checks=[
                pa.Check.greater_than_or_equal_to(1),
                pa.Check.less_than_or_equal_to(33051),
            ],
            nullable=False,
        ),
        "event_type": pa.Column(
            pl.String,
            checks=pa.Check.isin(["VOICE", "SMS", "DATA"]),
            nullable=False,
        ),
        "call_ts": pa.Column(pl.Datetime, nullable=False),
        "call_date": pa.Column(pl.String, nullable=False),
        "duration_sec": pa.Column(
            pl.Int64,
            checks=[
                pa.Check.greater_than_or_equal_to(0),
                pa.Check.less_than_or_equal_to(14400),
            ],
            nullable=False,
        ),
        "termination_code": pa.Column(
            pl.String,
            checks=pa.Check.isin(
                ["NORMAL", "HANDOVER_DROP", "SIGNAL_LOSS", "CONGESTION_DROP"]
            ),
            nullable=False,
        ),
        "is_dropped_call": pa.Column(pl.Boolean, nullable=False),
        "rsrp_dbm": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(-140.0),
                pa.Check.less_than_or_equal_to(-40.0),
            ],
            nullable=False,
        ),
        "rsrq_db": pa.Column(
            pl.Float64,
            checks=[
                pa.Check.greater_than_or_equal_to(-25.0),
                pa.Check.less_than_or_equal_to(0.0),
            ],
            nullable=False,
        ),
        "signal_quality": pa.Column(
            pl.String,
            checks=pa.Check.isin(["EXCELLENT", "GOOD", "FAIR", "POOR"]),
            nullable=False,
        ),
        "latency_ms": pa.Column(
            pl.Int64,
            checks=[
                pa.Check.greater_than_or_equal_to(1),
                pa.Check.less_than_or_equal_to(2000),
            ],
            nullable=False,
        ),
        "op": pa.Column(
            pl.String,
            checks=pa.Check.isin(["INSERT", "UPDATE", "DELETE"]),
            nullable=False,
        ),
        "updated_at_ts": pa.Column(pl.Datetime, nullable=False),
    },
    strict=False,
    coerce=True,
)
