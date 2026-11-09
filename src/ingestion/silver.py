"""Silver: typed, cleaned, single schema for both sources.

Idempotent per month: each month is rebuilt from bronze and written with
`replaceWhere year_month = ...`, so rerunning or republishing a month replaces
its rows instead of duplicating them.

Why batch + replaceWhere rather than streaming foreachBatch: a month is several
GB and a stream can split it across micro-batches, and each batch's
replaceWhere would then overwrite the previous batch's rows for that month.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import Config

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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
"""

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


# The SNOMED-era files renamed several columns. Note `bnf_chemical_substance`
# is a *code* in the legacy files but a *name* in the SNOMED files, so these
# renames must be applied together, in one pass, and only when the SNOMED
# header is present (detected by bnf_presentation_code, not by `src`).
SNOMED_RENAMES = {
    "bnf_chemical_substance": "chemical_substance_bnf_descr",
    "bnf_chemical_substance_code": "bnf_chemical_substance",
    "bnf_presentation_code": "bnf_code",
    "bnf_presentation_name": "bnf_description",
    "adq_usage": "adqusage",
}


def to_silver(df: DataFrame, src: str) -> DataFrame:
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    if "bnf_presentation_code" in df.columns:
        df = df.toDF(*[SNOMED_RENAMES.get(c, c) for c in df.columns])
    have = set(df.columns)

    # Legacy EPD files use YYYYMM; SNOMED EPD files use YYYY-MM. Tolerant
    # parsing so a malformed value is filtered below instead of failing the
    # query under ANSI mode.
    def year_month():
        value = "trim(cast(`year_month` AS STRING))"
        return F.expr(f"""
            coalesce(
              try_to_date({value}, 'yyyyMM'),
              try_to_date({value}, 'yyyy-MM'),
              try_to_date({value}, 'yyyy-MM-dd')
            )
        """).alias("year_month")

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
                year_month(),
                *[text(c) for c in STRING_COLS],
                *[number(c, t) for c, t in NUMERIC_COLS.items()],
                F.lit(src).alias("source"),
                F.col("_source_file"),
                F.col("_ingested_at"))
            .where(F.col("year_month").isNotNull()))


def latest_file_versions(df: DataFrame) -> DataFrame:
    """Keep only rows from the newest version of each source file.

    A republished file is re-ingested into bronze under the same path with a
    newer `_file_modified_at`. Null-safe equality keeps rows ingested before
    that column existed when no newer version is present.
    """
    if "_file_modified_at" not in df.columns:
        return df
    latest = df.groupBy("_source_file").agg(F.max("_file_modified_at").alias("_latest"))
    return (df.join(latest, "_source_file")
              .where(F.col("_file_modified_at").eqNullSafe(F.col("_latest")))
              .drop("_latest"))


def silver_month(spark: SparkSession, cfg: Config, month: str) -> int:
    """Rebuild one month of silver from bronze. Returns rows written."""
    src = cfg.source_for(month)
    if not spark.catalog.tableExists(cfg.bronze(src)):
        raise RuntimeError(f"Bronze table {cfg.bronze(src)} missing for {month}")

    month_start = f"{month[:4]}-{month[4:]}-01"
    raw = (spark.table(cfg.bronze(src))
                .where(F.col("_source_file").contains(f"epd_{month}")))
    df = (to_silver(latest_file_versions(raw), src)
          .where(F.col("year_month") == F.lit(month_start).cast("date")))

    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"year_month = DATE'{month_start}'")
        .saveAsTable(cfg.silver))
    return spark.table(cfg.silver).where(f"year_month = DATE'{month_start}'").count()


def silver(spark: SparkSession, cfg: Config) -> None:
    spark.sql(DDL.format(table=cfg.silver))
    for month in cfg.months():
        print(f"silver {month}: {silver_month(spark, cfg, month):,} rows")
