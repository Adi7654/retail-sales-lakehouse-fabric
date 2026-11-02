# Retail Sales Lakehouse on Microsoft Fabric

An end-to-end data engineering project that builds a **medallion-architecture Lakehouse** (bronze → silver → gold) in **Microsoft Fabric** on the Olist Brazilian e-commerce dataset, with incremental loads, a star-schema model, real-time order ingestion, and a Direct Lake Power BI report.

> 🚧 Status: in active development. Sections marked _TODO_ are filled in as each layer is completed.

---

## Architecture

![Architecture](docs/architecture.png)

```
GitHub (raw CSVs) ──► Data Factory pipeline ──► lh_bronze (raw files + Delta tables)
                                                    │  PySpark: types, dedup, quality checks
                                                    ▼
                                                lh_silver (clean Delta tables)
                                                    │  PySpark: Delta MERGE, SCD Type 2
                                                    ▼
                                                lh_gold (star schema) ──► Power BI (Direct Lake)

Live order simulator ──► Eventstream ──► Eventhouse ──► "Today's sales" view
```

## Tech Stack

| Area | Tools |
|------|-------|
| Platform | Microsoft Fabric (OneLake, Lakehouse) |
| Ingestion | Data Factory pipelines (ForEach + Copy, HTTP source) |
| Processing | PySpark notebooks, Delta Lake |
| Modeling | Star schema, SCD Type 2, Delta MERGE incremental loads |
| Streaming | Eventstream, Eventhouse (KQL) |
| Serving | Power BI semantic model (Direct Lake) |
| DevOps | Git integration, data quality checks |

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): about 100K orders (2016–2018) across 9 CSV files: customers, geolocation, orders, order items, payments, reviews, products, sellers, and a category translation lookup.

## Layers

### Bronze: raw ingestion
- `pl_ingest_bronze` pipeline loops over a parameterized file list (ForEach + Copy) and lands raw CSVs in `lh_bronze/Files/raw/olist/<load_date>/`.
- `nb_bronze_load` notebook writes each file to a `bronze_*` Delta table, kept as strings, with audit columns `_ingested_at` and `_source_file`.
- **Rows ingested:** _TODO_

### Silver: cleaned and validated
_TODO: type casting, deduplication, null handling, data quality checks_

### Gold: star schema
_TODO: fact_sales, dim_customer (SCD2), dim_product, dim_seller, dim_date; Delta MERGE incremental logic_

### Real-time
_TODO: Eventstream → Eventhouse_

### Power BI
_TODO: semantic model and report screenshots_

## Design Decisions

- **Separate Lakehouse per layer:** clear ownership, simpler permissions, easy to explain lineage.
- **Bronze kept as raw strings:** no data loss at ingestion; all typing and cleaning happens in silver, so bronze can always be replayed.
- **Parameterized ForEach pipeline:** adding a new source file is a one-line config change, not a new activity.
- _TODO: add more as built_

## Results

| Metric | Value |
|--------|-------|
| Total records processed | _TODO_ |
| Incremental vs full reload runtime | _TODO_ |
| Automated data quality checks | _TODO_ |

## Repository Structure

```
├── data/raw/      # source CSVs (pipeline pulls these over HTTP)
├── notebooks/     # exported Fabric notebooks
├── pipelines/     # pipeline JSON definitions and screenshots
└── docs/          # architecture diagram, screenshots
```

## Author

**Aditya Raj**: [LinkedIn](https://linkedin.com/in/aditya-raj-5a1a6a21b)
