"""Shared Auto Loader -> bronze helper for the reference datasets.

Same behaviour as bronze.py (all columns strings, `addNewColumns` evolution,
`allowOverwrites` so a republished file is re-ingested, lineage columns).
The EPD's own bronze.py is left as is for now; it could call this too.
"""
import os

from pyspark.sql import SparkSession, functions as F

from .config import Config


def load_to_bronze(spark: SparkSession, cfg: Config, *, name: str, landing: str,
                   table: str, fmt: str = "csv") -> None:
    if not os.path.isdir(landing) or not os.listdir(landing):
        print(f"No files in {landing}, skipping {name}")
        return
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.allowOverwrites", "true")
              .option("cloudFiles.schemaLocation", f"{cfg.vol}/_checkpoints/{name}/schema")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    if fmt == "csv":
        reader = reader.option("header", "true")
    (reader.load(landing)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{cfg.vol}/_checkpoints/{name}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table)
        .awaitTermination())
