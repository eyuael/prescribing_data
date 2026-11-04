# CLAUDE.md

## Project purpose

A data engineering practice project built on UK public healthcare data. The goal is to build a realistic, production-style pipeline on Databricks (PySpark + Delta Lake) that ingests NHS prescribing data, cleans it, and then enriches it with other public datasets. Several of these use different identifiers and change over time, so the project deliberately includes entity resolution, slowly changing dimensions and schema drift.

The core dataset is the **English Prescribing Dataset (EPD)** from the NHS Business Services Authority (NHSBSA): every prescription issued in English primary care and dispensed in the community, published monthly at practice × drug level.

This is a learning project, so clarity and good engineering practice matter more than cleverness. Explain trade-offs when suggesting changes.

## Data sources

| Source | Provider | Role | Join key |
|---|---|---|---|
| EPD (Jan 2014 – Jun 2025) | NHSBSA Open Data Portal, dataset `english-prescribing-data-epd` | Fact data (legacy) | – |
| EPD with SNOMED Code (Jul 2025 onwards) | NHSBSA, dataset `english-prescribing-dataset-epd-with-snomed-code` | Fact data (current) | – |
| Patients Registered at a GP Practice | NHS England | List sizes for per-patient metrics | `practice_code` |
| ODS (Organisation Data Service) | NHS England | Practice status, open/close dates, mergers/successions | `practice_code` |
| ONS Postcode Directory + English Indices of Deprivation | ONS / MHCLG | Geography (LSOA, local authority) and deprivation | `postcode` → LSOA |
| BNF Code Information | NHSBSA | Drug hierarchy and historic BNF code changes | `bnf_code` |

The NHSBSA portal runs on CKAN. The API base is `https://opendata.nhsbsa.net/api/3/action`; `package_show?id=<dataset>` lists each monthly file and its download URL.

## Architecture

Medallion layout in Unity Catalog, catalog `workspace`, schema `epd`:

- **Landing**: raw monthly CSVs in volume `/Volumes/workspace/epd/raw/landing/{legacy,snomed}/`, named `epd_YYYYMM.csv`. Files are downloaded to `_staging/` first and moved in only when complete.
- **Bronze**: `bronze_epd_legacy`, `bronze_epd_snomed`. Loaded by Auto Loader (`cloudFiles`, CSV, all columns as strings, `addNewColumns` schema evolution), plus `_source_file` and `_ingested_at`.
- **Silver**: `silver_epd`. One typed table for both sources, with an explicit DDL schema and `CLUSTER BY (year_month, practice_code)`. Codes and postcodes trimmed and uppercased, empty strings set to null, numbers parsed with `try_cast`, older STP columns mapped to ICB columns, `source` column records which dataset a row came from.
- **Gold**: `gold_practice_month` and `gold_bnf_chapter_month` aggregates.

Checkpoints and Auto Loader schema locations live under `/Volumes/workspace/epd/raw/_checkpoints/`.

## Current state

- `epd_pipeline.py`: a Databricks notebook in source format (`# COMMAND ----------` separates cells). It runs end to end for a configured month range (`START_MONTH`, `END_MONTH`) and has been tested successfully.
- The legacy/SNOMED cut-over month is set by `SNOMED_FROM = "202507"`.

## Steps / roadmap

1. ✅ Ingest EPD into landing via the CKAN API.
2. ✅ Bronze via Auto Loader.
3. ✅ Silver with a unified, typed schema and quality checks.
4. ✅ Gold aggregates by practice-month and BNF chapter-month.
5. ⬜ Make silver idempotent for republished months: `foreachBatch` with `MERGE` or `replaceWhere` on `year_month`, so a re-issued month replaces rather than duplicates.
6. ⬜ Refactor the notebook into a Python package with a script entry point, pytest unit tests, and a Databricks Asset Bundle job on a monthly schedule:
   ```
   src/epd/{ingest,bronze,silver,gold}.py
   jobs/run_monthly.py
   tests/test_silver.py
   databricks.yml
   ```
7. ⬜ Add patient list sizes; compute items and cost per 1,000 patients.
8. ⬜ Add ODS as a practice dimension (SCD Type 2), including a predecessor/successor table for merged practices.
9. ⬜ Add ONS Postcode Directory and deprivation data via practice postcode → LSOA.
10. ⬜ Add BNF Code Information to handle drug codes that changed over time.
11. ⬜ Optionally scale back to the full history from 2014.

## Conventions

- Column names are lowercase snake_case in silver and gold.
- Join on codes, never on names. Organisation names change (e.g. the ICB naming update from February 2026 data files), so keep names in dimension tables.
- Bronze keeps raw values as strings; all typing and cleaning happens in silver.
- New silver columns go into the explicit DDL first, then into `to_silver()`.
- Prefer incremental processing (Auto Loader, `availableNow` triggers) over full reloads.
- Keep transformation logic in pure functions that take and return DataFrames so they can be unit tested.

## Known gotchas

- Each month is several GB of CSV. Develop against a small month range before scaling up.
- If Auto Loader detects a new column, the stream stops on purpose. Rerunning continues with the new schema.
- Check silver column names against the NHSBSA EPD guidance document. A renamed source column does not error; it silently comes through as null. The quality-check query (null counts per month) catches this.
- The workspace may block outbound internet. If downloads fail, upload files manually into the `landing/` folders; everything downstream is unchanged.
- Prescriptions issued by dentists are not in the EPD.
