# Databricks notebook source
# MAGIC %md
# MAGIC # English Prescribing Dataset (EPD) pipeline
# MAGIC
# MAGIC **Landing** (raw CSVs in a Unity Catalog volume) → **Bronze** (Auto Loader, all strings) → **Silver** (typed, unified schema) → **Gold** (aggregates)
# MAGIC
# MAGIC Start with a small month range. Each month is several GB of CSV, so the full 2014+ history is very large.

# COMMAND ----------

import os, re, shutil, zipfile, requests
from pyspark.sql import functions as F

CATALOG = "workspace"   # default catalog on Databricks Free Edition; change if yours differs
SCHEMA  = "epd"
VOLUME  = "raw"

START_MONTH = "202501"  # inclusive, YYYYMM
END_MONTH   = "202512"  # inclusive, YYYYMM

# The original EPD stops at June 2025; later months are in the SNOMED version.
# Check the portal if this boundary looks wrong for your range.
SNOMED_FROM = "202507"

DATASETS = {
    "legacy": "english-prescribing-data-epd",
    "snomed": "english-prescribing-dataset-epd-with-snomed-code",
}
CKAN = "https://opendata.nhsbsa.net/api/3/action"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
for d in ["_staging", "_checkpoints"] + [f"landing/{s}" for s in DATASETS]:
    os.makedirs(f"{VOL}/{d}", exist_ok=True)

SILVER = f"{CATALOG}.{SCHEMA}.silver_epd"
def bronze_table(src): return f"{CATALOG}.{SCHEMA}.bronze_epd_{src}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Landing: download monthly files via the CKAN API
# MAGIC Files are written to a staging folder first and moved into `landing/` only when complete,
# MAGIC so Auto Loader never picks up a half-downloaded file.

# COMMAND ----------

MONTH_RE = re.compile(r"(20\d{2})(0[1-9]|1[0-2])")

def list_month_resources(dataset_id):
    """Return {YYYYMM: url} for the monthly CSV/ZIP resources of a dataset."""
    r = requests.get(f"{CKAN}/package_show", params={"id": dataset_id}, timeout=60)
    r.raise_for_status()
    out = {}
    for res in r.json()["result"]["resources"]:
        fmt = (res.get("format") or "").upper()
        m = MONTH_RE.search(res.get("name") or "") or MONTH_RE.search(res.get("url") or "")
        if m and fmt in ("CSV", "ZIP"):
            if m.group(0) not in out or fmt == "CSV":   # prefer CSV over ZIP
                out[m.group(0)] = res["url"]
    return out

def download_month(src, month, url):
    landing = f"{VOL}/landing/{src}"
    if any(f.startswith(f"epd_{month}") for f in os.listdir(landing)):
        return "already landed"

    tmp = f"{VOL}/_staging/{src}_{month}.part"
    with requests.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=16 * 1024 * 1024):
                f.write(chunk)

    if zipfile.is_zipfile(tmp):
        with zipfile.ZipFile(tmp) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            for i, name in enumerate(csvs):
                part = f"{VOL}/_staging/{src}_{month}_{i}.csv"
                with z.open(name) as zin, open(part, "wb") as zout:
                    shutil.copyfileobj(zin, zout, length=16 * 1024 * 1024)
                shutil.move(part, f"{landing}/epd_{month}_{i}.csv")
        os.remove(tmp)
    else:
        shutil.move(tmp, f"{landing}/epd_{month}.csv")
    return "downloaded"

for src, dataset_id in DATASETS.items():
    for month, url in sorted(list_month_resources(dataset_id).items()):
        in_range = START_MONTH <= month <= END_MONTH
        right_source = (src == "legacy") == (month < SNOMED_FROM)
        if in_range and right_source:
            print(src, month, download_month(src, month, url))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze: Auto Loader into one Delta table per source
# MAGIC Everything stays as strings; Auto Loader tracks which files it has already processed.
# MAGIC With `addNewColumns`, the stream stops when a new column appears. Rerun the cell and it continues with the new schema.

# COMMAND ----------

def load_bronze(src):
    landing = f"{VOL}/landing/{src}"
    if not os.listdir(landing):
        print(f"No files for {src}, skipping")
        return
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOL}/_checkpoints/bronze_{src}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{VOL}/_checkpoints/bronze_{src}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table(src))
        .awaitTermination())

for src in DATASETS:
    load_bronze(src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: typed, cleaned, single schema for both sources
# MAGIC The table is created up front so its schema is an explicit contract.
# MAGIC Check the column list against the EPD guidance document and adjust if NHSBSA has renamed anything.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  year_month DATE,
  regional_office_code STRING, regional_office_name STRING,
  icb_code STRING, icb_name STRING,
  pco_code STRING, pco_name STRING,
  practice_code STRING, practice_name STRING,
  address_1 STRING, address_2 STRING, address_3 STRING, address_4 STRING,
  postcode STRING,
  bnf_chemical_substance STRING, chemical_substance_bnf_descr STRING,
  bnf_code STRING, bnf_description STRING, bnf_chapter_plus_code STRING,
  snomed_code STRING,
  quantity DOUBLE, items BIGINT, total_quantity DOUBLE, adqusage DOUBLE,
  nic DOUBLE, actual_cost DOUBLE,
  source STRING, _source_file STRING, _ingested_at TIMESTAMP
)
CLUSTER BY (year_month, practice_code)
""")

STRING_COLS = [
    "regional_office_code", "regional_office_name", "icb_code", "icb_name",
    "pco_code", "pco_name", "practice_code", "practice_name",
    "address_1", "address_2", "address_3", "address_4", "postcode",
    "bnf_chemical_substance", "chemical_substance_bnf_descr",
    "bnf_code", "bnf_description", "bnf_chapter_plus_code", "snomed_code",
]
UPPERCASE_COLS = {"practice_code", "postcode", "icb_code", "pco_code", "bnf_code"}
NUMERIC_COLS = {
    "quantity": "DOUBLE", "items": "BIGINT", "total_quantity": "DOUBLE",
    "adqusage": "DOUBLE", "nic": "DOUBLE", "actual_cost": "DOUBLE",
}
# Older files used STP instead of ICB; take whichever exists.
FALLBACKS = {"icb_code": ["icb_code", "stp_code"], "icb_name": ["icb_name", "stp_name"]}

def to_silver(df, src):
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    have = set(df.columns)

    def text(name):
        present = [F.col(c) for c in FALLBACKS.get(name, [name]) if c in have]
        col = F.trim(F.coalesce(*present)) if present else F.lit(None).cast("string")
        col = F.upper(col) if name in UPPERCASE_COLS else col
        return F.when(col == "", None).otherwise(col).alias(name)

    def number(name, dtype):
        if name in have:
            return F.expr(f"try_cast(trim(`{name}`) AS {dtype})").alias(name)
        return F.lit(None).cast(dtype).alias(name)

    return (df.select(
                F.to_date(F.col("year_month"), "yyyyMM").alias("year_month"),
                *[text(c) for c in STRING_COLS],
                *[number(c, t) for c, t in NUMERIC_COLS.items()],
                F.lit(src).alias("source"),
                F.col("_source_file"),
                F.col("_ingested_at"))
            .where(F.col("year_month").isNotNull()))

def bronze_to_silver(src):
    if not spark.catalog.tableExists(bronze_table(src)):
        return
    (to_silver(spark.readStream.table(bronze_table(src)), src)
        .writeStream
        .option("checkpointLocation", f"{VOL}/_checkpoints/silver_{src}")
        .trigger(availableNow=True)
        .toTable(SILVER)
        .awaitTermination())

for src in DATASETS:
    bronze_to_silver(src)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality checks
# MAGIC Row counts and totals per month should be stable; sudden jumps usually mean a duplicate load or a schema problem.

# COMMAND ----------

display(spark.sql(f"""
SELECT year_month, source,
       count(*)                              AS row_count,
       sum(items)                            AS total_items,
       round(sum(actual_cost))               AS total_actual_cost,
       count_if(practice_code IS NULL)       AS null_practice_codes,
       count_if(items IS NULL)               AS unparsed_items,
       count(DISTINCT _source_file)          AS files
FROM {SILVER}
GROUP BY ALL
ORDER BY year_month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: analysis-ready aggregates

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_practice_month AS
SELECT year_month, practice_code,
       max(practice_name)          AS practice_name,
       max(postcode)               AS postcode,
       max(icb_code)               AS icb_code,
       sum(items)                  AS items,
       sum(actual_cost)            AS actual_cost,
       sum(nic)                    AS nic,
       count(DISTINCT bnf_code)    AS distinct_products
FROM {SILVER}
GROUP BY year_month, practice_code
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_bnf_chapter_month AS
SELECT year_month,
       substring(bnf_code, 1, 2)   AS bnf_chapter,
       sum(items)                  AS items,
       sum(actual_cost)            AS actual_cost
FROM {SILVER}
GROUP BY ALL
""")

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_bnf_chapter_month").orderBy("year_month", "bnf_chapter"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: free up storage
# MAGIC Once a month is safely in bronze, the raw CSV can go. Only run this if you're short on space.

# COMMAND ----------

# for src in DATASETS:
#     for f in os.listdir(f"{VOL}/landing/{src}"):
#         os.remove(f"{VOL}/landing/{src}/{f}")
