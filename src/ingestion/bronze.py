"""Bronze: Auto Loader into one Delta table per source. Everything stays a string.

With `addNewColumns`, the stream stops when a new column appears on purpose.
Rerunning continues with the new schema (retry the task).

`allowOverwrites` lets Auto Loader re-ingest a republished file that keeps its
name. Bronze is then an append-only log containing both versions; silver picks
the latest using `_file_modified_at`.
"""
import os

from pyspark.sql import SparkSession, functions as F

from .config import Config


def load_bronze(spark: SparkSession, cfg: Config, src: str) -> None:
    landing = cfg.landing(src)
    if not os.listdir(landing):
        print(f"No files for {src}, skipping")
        return
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.schemaLocation", f"{cfg.vol}/_checkpoints/bronze_{src}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{cfg.vol}/_checkpoints/bronze_{src}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(cfg.bronze(src))
        .awaitTermination())


def bronze(spark: SparkSession, cfg: Config) -> None:
    for src in cfg.datasets:
        load_bronze(spark, cfg, src)
