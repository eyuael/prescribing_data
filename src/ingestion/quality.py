"""Quality gate between silver and gold. Raises so the job fails before gold is touched."""
from pyspark.sql import SparkSession

from .config import Config

# A month whose row count moves more than this vs the previous month is suspect.
MAX_ROW_COUNT_CHANGE = 0.30
MAX_NULL_FRACTION = 0.001
# Columns that must be populated in every month. A renamed source column
# silently comes through as null, so all-null here means schema drift.
# Columns that may have some nulls but never all of them. Entirely null means
# a renamed source column (this is how the SNOMED header changes slipped in).
NOT_ALL_NULL_COLS = ["practice_name", "postcode", "bnf_description", "bnf_chemical_substance",
                     "chemical_substance_bnf_descr", "adqusage"]
REQUIRED_COLS = ["practice_code", "bnf_code", "items", "actual_cost", "nic"]


def check_month(spark: SparkSession, cfg: Config, month: str) -> list[str]:
    start = f"{month[:4]}-{month[4:]}-01"
    nulls = ",\n".join(f"count_if({c} IS NULL) AS null_{c}"
                       for c in REQUIRED_COLS + NOT_ALL_NULL_COLS)
    row = spark.sql(f"""
        SELECT count(*) AS n, {nulls}
        FROM {cfg.silver} WHERE year_month = DATE'{start}'
    """).first()

    if row["n"] == 0:
        return [f"{month}: no rows in silver"]

    problems = []
    for c in REQUIRED_COLS:
        frac = row[f"null_{c}"] / row["n"]
        if frac > MAX_NULL_FRACTION:
            problems.append(f"{month}: {c} is {frac:.2%} null (limit {MAX_NULL_FRACTION:.2%})")

    for c in NOT_ALL_NULL_COLS:
        if row[f"null_{c}"] == row["n"]:
            problems.append(f"{month}: {c} is entirely null (renamed source column?)")

    prev = spark.sql(f"""
        SELECT count(*) AS n FROM {cfg.silver}
        WHERE year_month = add_months(DATE'{start}', -1)
    """).first()["n"]
    if prev:
        change = abs(row["n"] - prev) / prev
        if change > MAX_ROW_COUNT_CHANGE:
            problems.append(f"{month}: row count {row['n']:,} vs previous month {prev:,} ({change:.0%} change)")
    return problems


def quality(spark: SparkSession, cfg: Config) -> None:
    problems = [p for m in cfg.months() for p in check_month(spark, cfg, m)]
    if problems:
        raise RuntimeError("Quality checks failed:\n  " + "\n  ".join(problems))
    print(f"Quality checks passed for {cfg.months()[0]}..{cfg.months()[-1]}")
