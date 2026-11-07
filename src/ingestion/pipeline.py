"""Pipeline entry point: ingest -> bronze -> silver -> quality -> gold.

Run locally against a Databricks workspace or as a job task:
    python -m ingestion.pipeline --start-month 202501 --end-month 202503
    python -m ingestion.pipeline            # latest published month only
    python -m ingestion.pipeline --stages silver quality gold   # rerun a subset
"""
import argparse

from pyspark.sql import SparkSession

from .bronze import bronze
from .config import Config
from .gold import gold
from .list_size import list_size
from .ingest import ingest, latest_published_month
from .quality import quality
from .silver import silver

STAGES = {
    "ingest": lambda spark, cfg: ingest(cfg),
    "bronze": bronze,
    "silver": silver,
    "quality": quality,
    "list_size": list_size,
    "gold": gold,
}


def run(spark: SparkSession, cfg: Config, stages: list[str] | None = None) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume}")
    for name in stages or STAGES:
        print(f"=== {name} ({cfg.start_month}..{cfg.end_month}) ===")
        STAGES[name](spark, cfg)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start-month", help="YYYYMM, inclusive. Default: latest published month")
    p.add_argument("--end-month", help="YYYYMM, inclusive. Default: start month, or latest published")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="epd")
    p.add_argument("--snomed-from", default="202507")
    p.add_argument("--stages", nargs="+", choices=list(STAGES), help="Run only these stages, in the given order")
    args = p.parse_args(argv)

    base = Config(start_month="000000", end_month="000000",
                  catalog=args.catalog, schema=args.schema, snomed_from=args.snomed_from)
    end = args.end_month or (args.start_month if args.start_month else latest_published_month(base))
    start = args.start_month or end
    cfg = Config(start_month=start, end_month=end,
                 catalog=args.catalog, schema=args.schema, snomed_from=args.snomed_from)

    spark = SparkSession.builder.getOrCreate()
    run(spark, cfg, args.stages)


if __name__ == "__main__":
    main()
