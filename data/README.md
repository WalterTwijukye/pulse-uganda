# 📁 Raw Data Assets Specification

This directory holds local raw source assets used during Bronze ingestion. To preserve repository agility and comply with standard DataOps practices, large operational raw files are excluded from Git version control via `.gitignore`.

---

## 📡 1. Cellular Mast Telemetry (`opencellID_ug_641.csv`)
* **Source:** [OpenCellID](https://opencellid.org/) (Crowd-sourced open mobile cell tower database)
* **License:** Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)
* **Scope:** Uganda (MCC = 641)
* **Format:** Raw headerless CSV export (14 columns)
* **Volume:** 33,051 records (~2.4 MB)
* **Schema Definition:**
  * Natural Composite Key: `(radio, mcc, net, area, cell)`
  * Spatial Coordinates: `lon` (Float64), `lat` (Float64)
  * Coverage Metric: `range` (Int64, estimated radius in meters)
  * Lineage Audit: `created`, `updated` (Unix epoch seconds)
* **Ingestion Destination:** Extracted via PostgreSQL OLTP (`oltp_tower_inventory`) into `s3://de-bronze/source=towers/` by `pipeline/ingest_towers.py`.

---

## 🗺️ 2. Uganda Administrative Boundaries (`uga_admin_boundaries.geojson/`)
* **Source:** [United Nations OCHA / Humanitarian Data Exchange (HDX)](https://data.humdata.org/dataset/cod-ab-uga)
* **License:** Creative Commons Attribution for Intergovernmental Organisations (CC BY-IGO)
* **Granularity Layers:**
  * `uga_admin0.geojson`: National boundary polygon (1 feature, 1.08 MB)
  * `uga_admin1.geojson`: Regional boundaries (4 features, 2.83 MB: Central, Eastern, Northern, Western)
  * `uga_admin2.geojson`: District boundaries (135 features, 18.59 MB)
  * `uga_admin3.geojson`: County boundaries (208 features, 21.56 MB)
  * `uga_admin4.geojson`: Parish boundaries (1,520 features, 48.90 MB)
* **Ingestion Destination:** Validated against `contracts/geo_schema.json` and uploaded to `s3://de-bronze/source=geo/` by `pipeline/ingest_geo.py`.
* **Spatial Processing:** Used in `pipeline/silver_towers.py` with Shapely `STRtree` spatial indexing to enrich tower coordinates with district and regional hierarchy.
