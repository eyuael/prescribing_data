"""Gold: analysis-ready aggregates, replaced per month so reruns are idempotent."""
from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import Config


def practice_month(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("year_month", "practice_code").agg(
        F.max("practice_name").alias("practice_name"),
        F.max("postcode").alias("postcode"),
        F.max("icb_code").alias("icb_code"),
        F.sum("items").alias("items"),
        F.sum("actual_cost").alias("actual_cost"),
        F.sum("nic").alias("nic"),
        F.countDistinct("bnf_code").alias("distinct_products"),
    ))


def with_list_size(pm: DataFrame, list_sizes: DataFrame) -> DataFrame:
    """Add list size and per-1,000-patient rates to a practice-month table.

    Left join on (year_month, practice_code): a practice with no list size
    (e.g. closed, or the list not yet published) keeps its row with null rates
    rather than disappearing. Zero patients also gives null, not a divide error.
    """
    ls = list_sizes.select("year_month", "practice_code", F.col("patients").alias("list_size"))
    denom = F.when(F.col("list_size") > 0, F.col("list_size") / 1000)
    return (pm.join(ls, ["year_month", "practice_code"], "left")
              .withColumn("items_per_1000", F.col("items") / denom)
              .withColumn("cost_per_1000", F.col("actual_cost") / denom))


def bnf_chapter_month(silver: DataFrame) -> DataFrame:
    return (silver
            .withColumn("bnf_chapter", F.substring("bnf_code", 1, 2))
            .groupBy("year_month", "bnf_chapter").agg(
                F.sum("items").alias("items"),
                F.sum("actual_cost").alias("actual_cost")))


def _replace_months(spark: SparkSession, df: DataFrame, table: str, months: list[str]) -> None:
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    dates = ", ".join(f"DATE'{m[:4]}-{m[4:]}-01'" for m in months)
    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"year_month IN ({dates})")
        .saveAsTable(table))


def gold(spark: SparkSession, cfg: Config) -> None:
    months = cfg.months()
    dates = [f"{m[:4]}-{m[4:]}-01" for m in months]
    silver = spark.table(cfg.silver).where(F.col("year_month").isin([F.lit(d).cast("date") for d in dates]))
    list_sizes = spark.table(cfg.silver_list_size)
    _replace_months(spark, with_list_size(practice_month(silver), list_sizes),
                    cfg.gold_practice_month, months)
    _replace_months(spark, bnf_chapter_month(silver), cfg.gold_bnf_chapter_month, months)
