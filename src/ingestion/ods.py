"""ODS (Organisation Data Service): practice dimension (SCD Type 2) + succession.

Source: the public ORD REST API (no auth), which replaced the old epraccur CSVs.
    list    GET /organisations?PrimaryRoleId=RO177&Status=Active|Inactive   (paged)
    detail  GET /organisations/<code>   (operational dates + successions)

RO177 is "prescribing cost centre", the role every EPD practice code has, so
this also covers walk-in centres and other non-GP cost centres.

Flow: snapshot JSONL in landing -> bronze_ods (an append-only log of snapshots)
-> dim_practice (SCD2, merged from the *latest* snapshot only)
 + dim_practice_succession (rebuilt each run; small).

History caveat: ODS only exposes its current state, so version history starts
at our first load. That first version gets effective_from = 1900-01-01 so
as-of joins work for every historical EPD month (we don't know what changed
before we started watching). Later changes get the snapshot date.
"""
import json
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from pyspark.sql import DataFrame, SparkSession, functions as F

from .autoloader import load_to_bronze
from .config import Config

ORD = "https://directory.spineservices.nhs.uk/ORD/2-0-0"
ROLE = "RO177"
STATUSES = ("Active", "Inactive")   # "Planned" orgs can't have prescribed yet
FIRST_VERSION_FROM = "1900-01-01"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; epd-pipeline)", "Accept": "application/json"}

# Attributes whose change creates a new dimension version.
TRACKED = ["practice_name", "postcode", "status", "primary_role_id",
           "operational_start", "operational_end"]

DIM_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  practice_code STRING, practice_name STRING, postcode STRING, status STRING,
  primary_role_id STRING, operational_start DATE, operational_end DATE,
  row_hash STRING, effective_from DATE, effective_to DATE, is_current BOOLEAN
)
CLUSTER BY (practice_code)
"""

SUCCS_SCHEMA = "array<struct<type:string,target:string,target_role:string,date:string>>"


# ---- landing (plain Python, no Spark) ------------------------------------

def parse_detail(detail: dict) -> dict:
    """Pull operational dates and successions out of an ORD organisation record."""
    org = detail["Organisation"]
    op = next((d for d in org.get("Date", []) if d.get("Type") == "Operational"), {})
    succs = []
    for s in (org.get("Succs") or {}).get("Succ", []):
        legal = next((d for d in s.get("Date", []) if d.get("Type") == "Legal"), {})
        target = s.get("Target", {})
        succs.append({
            "type": s.get("Type"),
            "target": (target.get("OrgId") or {}).get("extension"),
            "target_role": (target.get("PrimaryRoleId") or {}).get("id"),
            "date": legal.get("Start"),
        })
    return {"operational_start": op.get("Start"), "operational_end": op.get("End"),
            "succs_json": json.dumps(succs)}


def _get(session, url, retries=4):
    import time
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=60)
            r.raise_for_status()
            return r
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def list_orgs(session) -> list[dict]:
    out = []
    for status in STATUSES:
        url = f"{ORD}/organisations?PrimaryRoleId={ROLE}&Status={status}&Limit=1000"
        while url:
            r = _get(session, url)
            out += r.json()["Organisations"]
            url = r.headers.get("next-page")  # absent on the last page
    return out


def _previous_snapshot(landing: str) -> dict[str, dict]:
    """Last snapshot's records by org id, so unchanged orgs skip the detail call."""
    files = sorted(f for f in os.listdir(landing) if re.fullmatch(r"ods_\d{8}\.jsonl", f))
    if not files:
        return {}
    with open(f"{landing}/{files[-1]}") as fh:
        return {r["org_id"]: r for r in map(json.loads, fh)}


def ingest_ods(cfg: Config, today: date | None = None) -> None:
    import requests

    today = today or date.today()
    landing = cfg.landing("ods")
    os.makedirs(landing, exist_ok=True)
    os.makedirs(f"{cfg.vol}/_staging", exist_ok=True)
    target = f"{landing}/ods_{today:%Y%m%d}.jsonl"
    previous = _previous_snapshot(landing)

    session = requests.Session()
    session.headers.update(HEADERS)
    orgs = list_orgs(session)

    def build(o):
        rec = {"org_id": o["OrgId"], "name": o.get("Name"), "status": o.get("Status"),
               "postcode": o.get("PostCode"), "last_change_date": o.get("LastChangeDate"),
               "primary_role_id": o.get("PrimaryRoleId")}
        old = previous.get(rec["org_id"])
        if old and old.get("last_change_date") == rec["last_change_date"]:
            rec.update({k: old.get(k) for k in ("operational_start", "operational_end", "succs_json")})
        else:
            rec.update(parse_detail(_get(session, o["OrgLink"]).json()))
        return rec

    with ThreadPoolExecutor(8) as ex:
        records = list(ex.map(build, orgs))

    tmp = f"{cfg.vol}/_staging/ods_{today:%Y%m%d}.jsonl"
    with open(tmp, "w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")
    shutil.move(tmp, target)
    reused = sum(1 for r in records
                 if previous.get(r["org_id"], {}).get("last_change_date") == r["last_change_date"])
    print(f"ods snapshot {today}: {len(records):,} organisations, "
          f"{len(records) - reused:,} detail calls, {reused:,} reused")


# ---- silver (pure DataFrame functions) -----------------------------------

def to_silver_ods(df: DataFrame) -> DataFrame:
    """Typed, trimmed ODS snapshot rows. Codes uppercased, empty strings -> null."""
    def text(col, upper=False):
        c = F.trim(F.col(col).cast("string"))
        c = F.upper(c) if upper else c
        return F.when(c == "", None).otherwise(c)

    def dt(col):
        return F.expr(f"try_to_date(trim(cast(`{col}` AS STRING)), 'yyyy-MM-dd')")

    return df.select(
        text("org_id", upper=True).alias("practice_code"),
        text("name").alias("practice_name"),
        text("postcode", upper=True).alias("postcode"),
        text("status").alias("status"),
        text("primary_role_id", upper=True).alias("primary_role_id"),
        dt("operational_start").alias("operational_start"),
        dt("operational_end").alias("operational_end"),
        dt("last_change_date").alias("last_change_date"),
        F.col("succs_json"),
    ).where(F.col("practice_code").isNotNull())


def with_row_hash(df: DataFrame) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in TRACKED]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("␟", *parts), 256))


def scd2_stage(incoming: DataFrame, current: DataFrame, effective_from: str) -> DataFrame:
    """Rows to feed into the SCD2 MERGE.

    `incoming` is the typed snapshot (with row_hash); `current` is dim_practice
    (may be empty). Only new or changed practices are staged, each twice:
      merge_key = practice_code -> matches the open version so MERGE can close it
      merge_key = NULL          -> never matches, so MERGE inserts the new version
    Practices that are unchanged are not staged, which makes reruns no-ops.
    """
    open_versions = current.where("is_current").select("practice_code", F.col("row_hash").alias("_old_hash"))
    changed = (incoming.join(open_versions, "practice_code", "left")
               .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("row_hash")))
               .drop("_old_hash")
               .withColumn("effective_from", F.lit(effective_from).cast("date")))
    close = changed.withColumn("merge_key", F.col("practice_code"))
    insert = changed.withColumn("merge_key", F.lit(None).cast("string"))
    return close.unionByName(insert)


def to_succession(df: DataFrame) -> DataFrame:
    """One row per (predecessor, successor) pair from the snapshot's Succs data.

    ODS records a succession on both organisations: the closed one lists a
    'Successor', the surviving one lists a 'Predecessor'. We normalise both to
    (predecessor_code, successor_code) and dedupe.
    """
    parsed = (df.select("practice_code", F.explode(F.from_json("succs_json", SUCCS_SCHEMA)).alias("s"))
                .select("practice_code", F.col("s.*")))
    target = F.upper(F.trim("target"))
    pairs = (parsed
             .withColumn("target", target)
             .withColumn("predecessor_code", F.when(F.col("type") == "Predecessor", F.col("target"))
                         .otherwise(F.col("practice_code")))
             .withColumn("successor_code", F.when(F.col("type") == "Predecessor", F.col("practice_code"))
                         .otherwise(F.col("target")))
             .where(F.col("type").isin("Predecessor", "Successor") & F.col("target").isNotNull()))
    return (pairs.select("predecessor_code", "successor_code",
                         F.expr("try_to_date(date, 'yyyy-MM-dd')").alias("succession_date"),
                         F.col("target_role").alias("other_party_role_id"))
                 .distinct())


def latest_snapshot(bronze: DataFrame) -> DataFrame:
    """Rows from the newest ods_YYYYMMDD.jsonl file (and its newest version)."""
    snap = F.regexp_extract("_source_file", r"ods_(\d{8})\.jsonl", 1)
    b = bronze.withColumn("_snapshot", snap).where(F.col("_snapshot") != "")
    newest = b.agg(F.max("_snapshot")).first()[0]
    return b.where(F.col("_snapshot") == newest)


# ---- silver (tables) -----------------------------------------------------

def silver_ods(spark: SparkSession, cfg: Config) -> None:
    if not spark.catalog.tableExists(cfg.bronze_ods):
        raise RuntimeError(f"{cfg.bronze_ods} missing; run ODS ingest and bronze first")
    spark.sql(DIM_DDL.format(table=cfg.dim_practice))

    from .silver import latest_file_versions  # a same-day rerun lands a second version of the file

    raw = latest_file_versions(latest_snapshot(spark.table(cfg.bronze_ods)))
    snapshot = raw.agg(F.max("_snapshot")).first()[0]
    snapshot_date = f"{snapshot[:4]}-{snapshot[4:6]}-{snapshot[6:]}"
    # No .cache(): unsupported on serverless environment v2, and the snapshot is tiny.
    silver = to_silver_ods(raw)
    n = silver.count()
    if n == 0:
        raise RuntimeError("Latest ODS snapshot is empty")
    # ODS has one row per code; a duplicate would corrupt the MERGE.
    dup = silver.groupBy("practice_code").count().where("count > 1").count()
    if dup:
        raise RuntimeError(f"{dup} duplicate practice codes in ODS snapshot {snapshot}")

    current = spark.table(cfg.dim_practice)
    first_load = current.limit(1).count() == 0
    stage = scd2_stage(with_row_hash(silver.drop("last_change_date", "succs_json")), current,
                       FIRST_VERSION_FROM if first_load else snapshot_date)
    stage.createOrReplaceTempView("ods_stage")
    cols = ["practice_code", "practice_name", "postcode", "status", "primary_role_id",
            "operational_start", "operational_end", "row_hash", "effective_from"]
    spark.sql(f"""
        MERGE INTO {cfg.dim_practice} t
        USING ods_stage s
        ON t.practice_code = s.merge_key AND t.is_current
        WHEN MATCHED THEN UPDATE SET t.is_current = false, t.effective_to = s.effective_from
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
          ({", ".join(cols)}, effective_to, is_current)
          VALUES ({", ".join("s." + c for c in cols)}, NULL, true)
    """)
    print(f"dim_practice: snapshot {snapshot_date}, {n:,} orgs, "
          f"{spark.table(cfg.dim_practice).where('is_current').count():,} current versions")

    (to_succession(silver).write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(cfg.dim_practice_succession))
    print(f"dim_practice_succession: {spark.table(cfg.dim_practice_succession).count():,} pairs")


def ods(spark: SparkSession, cfg: Config) -> None:
    """Landing -> bronze -> dim_practice + dim_practice_succession."""
    ingest_ods(cfg)
    load_to_bronze(spark, cfg, name="bronze_ods", landing=cfg.landing("ods"),
                   table=cfg.bronze_ods, fmt="json")
    silver_ods(spark, cfg)
