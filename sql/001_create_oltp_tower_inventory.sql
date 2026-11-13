-- 001: OLTP tower inventory (Source A simulation) — mirrors OpenCellID 14-col schema
CREATE TABLE oltp_tower_inventory (
    tower_id        BIGSERIAL PRIMARY KEY,
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
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_oltp_tower_inventory_natural_key
        UNIQUE (mcc, net, area, cell)
);