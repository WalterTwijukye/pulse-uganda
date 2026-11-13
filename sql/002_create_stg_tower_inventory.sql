-- 002: Staging table for raw OpenCellID load — receives everything, constrains nothing
CREATE TABLE IF NOT EXISTS stg_tower_inventory_raw (
    radio           VARCHAR(10) NOT NULL,
    mcc             SMALLINT NOT NULL,
    net             SMALLINT NOT NULL,
    area            INTEGER NOT NULL,
    cell            BIGINT NOT NULL,
    unit            SMALLINT,
    lon             DOUBLE PRECISION,
    lat             DOUBLE PRECISION NOT NULL,
    "range"         INTEGER,
    samples         INTEGER,
    changeable      SMALLINT,
    created         BIGINT,
    updated         BIGINT,
    average_signal  SMALLINT,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
