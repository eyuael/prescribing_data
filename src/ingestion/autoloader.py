"""Shared Auto Loader -> bronze helper for the reference datasets.

Same behaviour as bronze.py (all columns strings, `addNewColumns` evolution,
`allowOverwrites` so a republished file is re-ingested, lineage columns).
The EPD's own bronze.py is left as is for now; it could call this too.
"""
import os
import re

from pyspark.sql import SparkSession, functions as F

from .config import Config


def slug(name: str) -> str:
    """'Income Decile (where 1 is most deprived)' -> 'income_decile_where_1_is_most_deprived'.

    Delta rejects spaces and ,;{}()=  in column names; this is a deterministic
    rename, so no information in the values is lost.
    """
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def load_to_bronze(spark: SparkSession, cfg: Config, *, name: str, landing: str,
                   table: str, fmt: str = "csv", sanitize_columns: bool = False) -> None:
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
    df = reader.load(landing)
    if sanitize_columns:   # opt-in: renaming would change columns of already-loaded tables
        df = df.toDF(*[slug(c) for c in df.columns])
    (df
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{cfg.vol}/_checkpoints/{name}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table)
        .awaitTermination())
