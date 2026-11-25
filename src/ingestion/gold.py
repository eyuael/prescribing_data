"""Gold: analysis-ready aggregates, replaced per month so reruns are idempotent."""
from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import Config
from .geography import normalise_postcode


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


def with_practice_context(pm: DataFrame, dim_practice: DataFrame,
                          onspd: DataFrame, imd: DataFrame) -> DataFrame:
    """Add ODS status, LSOA, local authority and deprivation to a practice-month table.

    - dim_practice is joined *as of the month* (SCD2 range join), so a practice
      that has since closed still shows the status it had then.
    - Postcode comes from the EPD row, falling back to ODS when the EPD has none.
    - All joins are left joins on unique keys, so row count never changes;
      practices we can't place just get nulls (e.g. Welsh postcodes, retired codes).
    """
    d = dim_practice.select(
        F.col("practice_code").alias("_d_code"), "effective_from", "effective_to",
        F.col("status").alias("ods_status"), F.col("postcode").alias("_ods_postcode"))
    in_range = ((pm["year_month"] >= d["effective_from"])
                & (d["effective_to"].isNull() | (pm["year_month"] < d["effective_to"])))
    j = (pm.join(d, (pm["practice_code"] == d["_d_code"]) & in_range, "left")
           .drop("_d_code", "effective_from", "effective_to")
           .withColumn("_pc_key", normalise_postcode(F.coalesce("postcode", "_ods_postcode")))
           .drop("_ods_postcode"))
    geo = onspd.select(F.col("postcode_key").alias("_pc_key"), "lsoa21_code",
                       "lad_code", F.col("icb_code").alias("onspd_icb_code"))
    deprivation = imd.select("lsoa21_code", "imd_score", "imd_rank", "imd_decile")
    return (j.join(geo, "_pc_key", "left")
             .join(deprivation, "lsoa21_code", "left")
             .drop("_pc_key"))


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
    # mergeSchema: gold gains columns as enrichments are added (list size, ODS,
    # geography). Rows for months not rebuilt keep nulls in the new columns
    # until those months are rerun.
    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"year_month IN ({dates})")
        .option("mergeSchema", "true")
        .saveAsTable(table))


def gold(spark: SparkSession, cfg: Config) -> None:
    months = cfg.months()
    dates = [f"{m[:4]}-{m[4:]}-01" for m in months]
    silver = spark.table(cfg.silver).where(F.col("year_month").isin([F.lit(d).cast("date") for d in dates]))
    list_sizes = spark.table(cfg.silver_list_size)
    pm = with_practice_context(
        with_list_size(practice_month(silver), list_sizes),
        spark.table(cfg.dim_practice), spark.table(cfg.silver_onspd), spark.table(cfg.silver_imd))
    _replace_months(spark, pm, cfg.gold_practice_month, months)
    _report_geography_coverage(spark, cfg, months)
    _report_bnf_coverage(spark, cfg, months)
    _replace_months(spark, bnf_chapter_month(silver), cfg.gold_bnf_chapter_month, months)


def _report_geography_coverage(spark: SparkSession, cfg: Config, months: list[str]) -> None:
    """Print the share of practices with an LSOA and a deprivation score.

    Printed rather than enforced: some practices legitimately have no match
    (Welsh border postcodes, retired codes). A sudden drop is the signal.
    """
    dates = ", ".join(f"DATE'{m[:4]}-{m[4:]}-01'" for m in months)
    row = spark.sql(f"""
        SELECT count(*) AS n, count_if(lsoa21_code IS NULL) AS no_lsoa,
               count_if(imd_decile IS NULL) AS no_imd, count_if(ods_status IS NULL) AS no_ods
        FROM {cfg.gold_practice_month} WHERE year_month IN ({dates})""").first()
    n = max(row["n"], 1)
    print(f"gold coverage over {row['n']:,} practice-months: no ODS {row['no_ods']/n:.1%}, "
          f"no LSOA {row['no_lsoa']/n:.1%}, no deprivation {row['no_imd']/n:.1%}")


def _report_bnf_coverage(spark: SparkSession, cfg: Config, months: list[str]) -> None:
    """Print, per month, the share of items whose bnf_code is in dim_bnf, and how many items sit on
    retired codes. A month near 0% means bnf_code itself is null (renamed source column);
    a drop in a normal month means dim_bnf is stale (rerun the bnf stage)."""
    if not spark.catalog.tableExists(cfg.dim_bnf):
        print("bnf coverage: dim_bnf not built yet, skipped")
        return
    dates = ", ".join(f"DATE'{m[:4]}-{m[4:]}-01'" for m in months)
    rows = spark.sql(f"""
        SELECT s.year_month,
               sum(s.items)                                              AS items,
               sum(CASE WHEN d.presentation_code IS NOT NULL THEN s.items END)           AS matched,
               sum(CASE WHEN d.lineage_method = 'same_name' THEN s.items END)            AS recoded,
               sum(CASE WHEN d.lineage_method = 'retired_no_successor' THEN s.items END) AS orphaned
        FROM {cfg.silver} s LEFT JOIN {cfg.dim_bnf} d ON s.bnf_code = d.presentation_code
        WHERE s.year_month IN ({dates}) GROUP BY s.year_month ORDER BY s.year_month""").collect()
    for r in rows:
        n = max(r["items"] or 0, 1)
        print(f"bnf coverage {r['year_month']}: {(r['matched'] or 0) / n:.2%} of items in dim_bnf; "
              f"{(r['recoded'] or 0) / n:.2%} on recoded codes, {(r['orphaned'] or 0) / n:.2%} on retired codes with no successor")
