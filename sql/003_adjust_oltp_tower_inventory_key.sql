-- 003: Adjust natural key constraint to include radio access technology (RAT)
-- Multi-RAT co-location (GSM/UMTS/LTE sharing cell IDs) requires radio in the natural key

ALTER TABLE oltp_tower_inventory
    DROP CONSTRAINT uq_oltp_tower_inventory_natural_key;

ALTER TABLE oltp_tower_inventory
    ADD CONSTRAINT uq_oltp_tower_inventory_natural_key
    UNIQUE (radio, mcc, net, area, cell);
