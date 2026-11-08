"""Patients Registered at a GP Practice (NHS England): practice list sizes.

Published monthly as a snapshot at the 1st of the month. We use the
all-persons practice file (`gp-reg-pat-prac-all`), one row per practice.

Unlike NHSBSA there is no API, and download URLs contain hashed folders
(`files.digital.nhs.uk/FB/7851F6/...`), so they can't be built from the month.
We scrape the monthly publication page for the link instead.

Flow mirrors the EPD: landing CSV -> bronze (Auto Loader, strings) -> silver
(typed, replaced per month) so every stage is idempotent.
"""
import calendar
import io
import os
import re
import shutil
import zipfile

from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import Config

PUBLICATION_URL = ("https://digital.nhs.uk/data-and-information/publications/"
                   "statistical/patients-registered-at-a-gp-practice/{month_name}-{year}")
ZIP_NAME = "gp-reg-pat-prac-all.zip"
# digital.nhs.uk can reject the default python-requests User-Agent.
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; epd-pipeline)"}
LINK_RE = re.compile(r'https://files\.digital\.nhs\.uk/[^"\'\s<>]*' + re.escape(ZIP_NAME))

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  year_month DATE, practice_code STRING, postcode STRING,
  sub_icb_location_code STRING, patients BIGINT,
  _source_file STRING, _ingested_at TIMESTAMP
)
CLUSTER BY (year_month, practice_code)
"""


# ---- landing -------------------------------------------------------------

def publication_url(month: str) -> str:
    return PUBLICATION_URL.format(month_name=calendar.month_name[int(month[4:])].lower(),
                                  year=month[:4])


def find_zip_url(html: str) -> str | None:
    """Pull the all-persons practice zip link out of a publication page."""
    m = LINK_RE.search(html)
    return m.group(0) if m else None


def ingest_list_size(cfg: Config) -> None:
    import requests  # only the landing stage needs it

    landing = cfg.landing("list_size")
    for d in ("_staging", "landing/list_size"):
        os.makedirs(f"{cfg.vol}/{d}", exist_ok=True)

    missing = []
    for month in cfg.months():
        target = f"{landing}/list_size_{month}.csv"
        if os.path.exists(target):
            print("list_size", month, "already landed")
            continue
        page_url = publication_url(month)
        page = requests.get(page_url, timeout=60, headers=HEADERS)
        if not page.ok:
            # A blocked or failing request is not the same as "not published",
            # so fail with the real status instead of hiding it.
            raise RuntimeError(f"{page_url} returned HTTP {page.status_code} "
                               f"(body starts: {page.text[:200]!r})")
        url = find_zip_url(page.text)
        if url is None:
            missing.append(month)
            continue
        resp = requests.get(url, timeout=300, headers=HEADERS)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            tmp = f"{cfg.vol}/_staging/list_size_{month}.csv"
            with z.open(name) as zin, open(tmp, "wb") as zout:
                shutil.copyfileobj(zin, zout)
        shutil.move(tmp, target)  # staging first, so Auto Loader never sees a partial file
        print("list_size", month, "downloaded")
    if missing:
        raise RuntimeError(f"List size not published yet: {', '.join(missing)}")


# ---- bronze --------------------------------------------------------------

def bronze_list_size(spark: SparkSession, cfg: Config) -> None:
    landing = cfg.landing("list_size")
    if not os.path.isdir(landing) or not os.listdir(landing):
        print("No list size files, skipping")
        return
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.schemaLocation", f"{cfg.vol}/_checkpoints/bronze_list_size/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{cfg.vol}/_checkpoints/bronze_list_size")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(cfg.bronze_list_size)
        .awaitTermination())


# ---- silver --------------------------------------------------------------

def to_silver_list_size(df: DataFrame) -> DataFrame:
    """Typed practice list sizes: one row per practice per month.

    Keeps only practice rows (TYPE = 'GP') for all sexes and ages. A renamed
    source column would come through null, so the quality check also covers
    `patients`.
    """
    df = df.toDF(*[c.strip().lower() for c in df.columns])

    def code(name):
        c = F.upper(F.trim(F.col(name)))
        return F.when(c == "", None).otherwise(c).alias(name)

    return (df
            .where((F.upper(F.trim("type")) == "GP")
                   & (F.upper(F.trim("sex")) == "ALL")
                   & (F.upper(F.trim("age")) == "ALL"))
            .select(
                F.expr("date_trunc('MONTH', try_to_date(trim(extract_date), 'yyyy-MM-dd'))")
                 .cast("date").alias("year_month"),
                code("code").alias("practice_code"),
                code("postcode"),
                code("sub_icb_location_code"),
                F.expr("try_cast(trim(number_of_patients) AS BIGINT)").alias("patients"),
                F.col("_source_file"), F.col("_ingested_at"))
            .where(F.col("year_month").isNotNull() & F.col("practice_code").isNotNull()))


def silver_list_size(spark: SparkSession, cfg: Config) -> None:
    from .silver import latest_file_versions

    spark.sql(SILVER_DDL.format(table=cfg.silver_list_size))
    if not spark.catalog.tableExists(cfg.bronze_list_size):
        raise RuntimeError(f"{cfg.bronze_list_size} missing; run the list_size ingest and bronze first")
    for month in cfg.months():
        start = f"{month[:4]}-{month[4:]}-01"
        raw = (spark.table(cfg.bronze_list_size)
                    .where(F.col("_source_file").contains(f"list_size_{month}")))
        df = (to_silver_list_size(latest_file_versions(raw))
              .where(F.col("year_month") == F.lit(start).cast("date")))
        (df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"year_month = DATE'{start}'")
            .saveAsTable(cfg.silver_list_size))
        n = spark.table(cfg.silver_list_size).where(f"year_month = DATE'{start}'").count()
        print(f"silver_list_size {month}: {n:,} practices")
        if n == 0:
            raise RuntimeError(f"No list size rows for {month}")


def list_size(spark: SparkSession, cfg: Config) -> None:
    """Landing -> bronze -> silver for list sizes."""
    ingest_list_size(cfg)
    bronze_list_size(spark, cfg)
    silver_list_size(spark, cfg)
