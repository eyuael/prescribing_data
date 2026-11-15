"""Export: publish gold data as static files for the browser (DuckDB-WASM).

The web app queries Parquet directly in the browser, so there is no API and no
Databricks compute at view time. Gold is tiny (about 9,000 practice rows a
month), so the whole table is rewritten on every run: one file, no
incremental logic, and it stays correct when a month is republished.

Files (in the volume at <vol>/export/, and in the R2 bucket if configured):
    practice_month.parquet   one row per practice per month, only the columns the charts use
    manifest.json            written LAST, so it never points at data that isn't there yet

Names stay out of the fact file (join on codes, names live in dimensions).
Rates are NOT exported: the browser computes sum(items) / sum(list_size) per
group. Averaging per-practice rates would weight a 1,000-patient practice the
same as a 20,000-patient one.

R2 credentials come from env vars or the Databricks secret scope `epd`:
    r2_account_id, r2_access_key_id, r2_secret_access_key, r2_bucket
Without them the files are still written to the volume and upload is skipped
(download them with `databricks fs cp` and upload by hand).
"""
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from .config import Config

EXPORT_COLUMNS = ["year_month", "practice_code", "imd_decile", "list_size", "items", "actual_cost"]
SECRET_SCOPE = "epd"
SECRET_KEYS = ["r2_account_id", "r2_access_key_id", "r2_secret_access_key", "r2_bucket"]
CACHE_CONTROL = "public, max-age=300"   # five minutes: fresh enough, and cheap


def practice_month_export(gold: DataFrame) -> DataFrame:
    """The columns the charts need, in a stable order (stable order compresses better)."""
    return gold.select(*EXPORT_COLUMNS).orderBy("year_month", "practice_code")


def build_manifest(months: list[str], rows: int, generated_at: str) -> dict:
    return {"generated_at": generated_at, "rows": rows, "months": months,
            "files": {"practice_month": "practice_month.parquet"}}


def _setting(spark: SparkSession, key: str) -> str | None:
    value = os.environ.get(key.upper())
    if value:
        return value
    try:   # Databricks secrets; unavailable (or scope missing) when running locally
        from pyspark.dbutils import DBUtils
        return DBUtils(spark).secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        return None


def upload_to_r2(files: list[tuple[str, str, str]], account_id: str, key_id: str,
                 secret: str, bucket: str) -> None:
    """files = [(local_path, object_key, content_type)], uploaded in the order given."""
    import boto3

    s3 = boto3.client("s3", endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
                      aws_access_key_id=key_id, aws_secret_access_key=secret, region_name="auto")
    for path, key, content_type in files:
        s3.upload_file(path, bucket, key,
                       ExtraArgs={"ContentType": content_type, "CacheControl": CACHE_CONTROL})
        print(f"uploaded {key} ({os.path.getsize(path):,} bytes)")


def export(spark: SparkSession, cfg: Config) -> None:
    out = f"{cfg.vol}/export"
    os.makedirs(out, exist_ok=True)

    pdf = practice_month_export(spark.table(cfg.gold_practice_month)).toPandas()
    if pdf.empty:
        raise RuntimeError(f"{cfg.gold_practice_month} is empty; nothing to export")
    # Spark nulls come through as floats; keep whole numbers whole.
    for c in ("imd_decile", "list_size", "items"):
        pdf[c] = pdf[c].astype("Int64")

    parquet_path = f"{out}/practice_month.parquet"
    pdf.to_parquet(parquet_path, index=False)

    months = sorted(pd_month.strftime("%Y-%m") for pd_month in pdf["year_month"].unique())
    manifest_path = f"{out}/manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(build_manifest(months, len(pdf), datetime.now(timezone.utc).isoformat(timespec="seconds")),
                  f, indent=2)
    print(f"export: {len(pdf):,} rows, {len(months)} months ({months[0]}..{months[-1]}) -> {out}")

    creds = {k: _setting(spark, k) for k in SECRET_KEYS}
    if not all(creds.values()):
        missing = [k for k, v in creds.items() if not v]
        print(f"export: R2 not configured (missing {', '.join(missing)}); upload skipped. "
              f"Files are in {out}")
        return
    upload_to_r2(
        [(parquet_path, "practice_month.parquet", "application/octet-stream"),
         (manifest_path, "manifest.json", "application/json")],   # manifest last
        creds["r2_account_id"], creds["r2_access_key_id"],
        creds["r2_secret_access_key"], creds["r2_bucket"])
