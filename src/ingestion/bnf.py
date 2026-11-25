"""BNF Code Information (NHSBSA): drug hierarchy and codes that changed over time.

Datasets on the NHSBSA CKAN portal:
    bnf-code-information-historic       one FINAL snapshot per past year (2020 onwards)
    bnf-code-information-current-year   one FULL snapshot per month of the current year
    (bnf-code-information-monthly-changes lists ADD/CHANGE/REMOVE rows but gives no
     old -> new mapping, so we derive that ourselves, see below.)

Each snapshot lists every presentation with its whole hierarchy:
    chapter(2) > section(4) > paragraph(6) > subparagraph(7) > chemical substance(9)
    > product(11) > presentation(15 characters)
so a medicine group is just a code prefix (antipsychotics = paragraph 040201).

Why not just join EPD to the latest snapshot? Codes do get retired: between the
Dec 2025 and Aug 2026 snapshots the beclometasone/formoterol inhalers (Fostair,
Luforbec, ...) moved from 0302000C0... to 0302000A..., which is 0.7% of the items
in Jan 2025. A series by chemical substance would break at the recode.

dim_bnf therefore has one row per presentation code EVER seen:
  * retired codes keep their last-known hierarchy, so old months still resolve;
  * `current_presentation_code` points a retired code at its successor when exactly
    one current code has the identical presentation name (the NHSBSA gives no
    mapping, so this is a heuristic, recorded in `lineage_method`);
  * the hierarchy columns come from the successor when there is one, so history is
    expressed in TODAY's groups.
Retired codes with no unique successor stay under their own last-known hierarchy.
"""
import os
import re
import shutil

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from .autoloader import load_to_bronze
from .config import CKAN, Config

DATASETS = {
    "historic": "bnf-code-information-historic",
    "current": "bnf-code-information-current-year",
}
# 'BNF_CODE_HISTORIC_2025_VERSION_88' or 'BNF_CODE_CURRENT_202610_VERSION_90'
RESOURCE_RE = re.compile(r"BNF_CODE_(HISTORIC|CURRENT)_(\d{4,6})_VERSION_(\d+)", re.I)

LEVELS = ["chapter", "section", "paragraph", "subparagraph", "chemical_substance", "product"]


# ---- landing -------------------------------------------------------------

def parse_resource_name(name: str) -> dict | None:
    """-> {'kind', 'period', 'version', 'snapshot_key'} or None. Historic year Y is the Dec-Y state."""
    m = RESOURCE_RE.search(name or "")
    if not m:
        return None
    kind, period, version = m.group(1).lower(), m.group(2), int(m.group(3))
    key = f"{period}-12" if kind == "historic" else f"{period[:4]}-{period[4:]}"
    return {"kind": kind, "period": period, "version": version, "snapshot_key": key}


def landing_name(info: dict) -> str:
    return f"bnf_{info['kind']}_{info['period']}_v{info['version']}.csv"


def plan_downloads(resources: list[dict]) -> list[tuple[dict, str]]:
    """Which files we want: every historic year, and only the newest current-year month
    (each current file is a complete snapshot, older months add nothing we need)."""
    found = [(i, r["url"]) for r in resources if (i := parse_resource_name(r.get("name"))) and r.get("url")]
    historic = [(i, u) for i, u in found if i["kind"] == "historic"]
    current = sorted((x for x in found if x[0]["kind"] == "current"), key=lambda x: x[0]["period"])
    return historic + current[-1:]


def ingest_bnf(cfg: Config) -> None:
    import requests

    os.makedirs(f"{cfg.vol}/_staging", exist_ok=True)
    for kind in DATASETS:
        os.makedirs(cfg.landing(f"bnf_{kind}"), exist_ok=True)
    for ds in DATASETS.values():
        r = requests.get(f"{CKAN}/package_show", params={"id": ds}, timeout=60)
        r.raise_for_status()
        for info, url in plan_downloads(r.json()["result"]["resources"]):
            name = landing_name(info)
            landing = cfg.landing(f"bnf_{info['kind']}")
            # A republished version keeps its period but bumps `version`, so it lands as a new file.
            if name in os.listdir(landing):
                continue
            tmp = f"{cfg.vol}/_staging/{name}.part"
            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
            shutil.move(tmp, f"{landing}/{name}")
            print("bnf", name, "downloaded")


# ---- silver (pure DataFrame functions) -----------------------------------

def to_silver_bnf(df: DataFrame) -> DataFrame:
    """Typed, trimmed snapshot rows. The snapshot comes from the file name, not a column
    (historic files have no date column)."""
    df = df.toDF(*[c.strip().lower() if not c.startswith("_") else c for c in df.columns])
    f = F.col("_source_file")
    kind = F.regexp_extract(f, r"bnf_(historic|current)_(\d{4,6})_v(\d+)\.csv", 1)
    period = F.regexp_extract(f, r"bnf_(historic|current)_(\d{4,6})_v(\d+)\.csv", 2)
    version = F.regexp_extract(f, r"bnf_(historic|current)_(\d{4,6})_v(\d+)\.csv", 3)

    def text(col, upper=False):
        c = F.trim(F.col(f"bnf_{col}"))
        c = F.upper(c) if upper else c
        return F.when(c == "", None).otherwise(c)

    cols = []
    for lvl in LEVELS:
        cols += [text(f"{lvl}_code", upper=True).alias(f"{lvl}_code"), text(lvl).alias(f"{lvl}_name")]
    cols += [text("presentation_code", upper=True).alias("presentation_code"),
             text("presentation").alias("presentation_name")]
    return (df.select(
                F.when(kind == "historic", F.concat(period, F.lit("-12")))
                 .otherwise(F.concat(F.substring(period, 1, 4), F.lit("-"), F.substring(period, 5, 2)))
                 .alias("snapshot_key"),
                kind.alias("snapshot_kind"), version.cast("int").alias("version"),
                *cols, f.alias("_source_file"))
            .where((kind != "") & F.col("presentation_code").isNotNull()))


def normalise_name(col):
    """'Fostair 100micrograms/dose / 6micrograms/dose inhaler' -> lowercase words, punctuation-free."""
    return F.trim(F.regexp_replace(F.lower(col), r"[^a-z0-9]+", " "))


def build_dim_bnf(snaps: DataFrame) -> DataFrame:
    """One row per presentation code ever seen. See the module docstring for the rules."""
    latest_key = snaps.agg(F.max("snapshot_key")).first()[0]

    w = Window.partitionBy("presentation_code")
    ranked = (snaps.withColumn("_first", F.min("snapshot_key").over(w))
                   .withColumn("_rn", F.row_number().over(w.orderBy(F.col("snapshot_key").desc(), F.col("version").desc())))
                   .where("_rn = 1").drop("_rn"))          # last-seen row per code
    base = (ranked.withColumn("first_seen_snapshot", F.col("_first"))
                  .withColumn("last_seen_snapshot", F.col("snapshot_key"))
                  .withColumn("is_current", F.col("snapshot_key") == F.lit(latest_key))
                  .drop("_first", "snapshot_key", "snapshot_kind", "version", "_source_file"))

    # A retired code's successor: the single current presentation with an identical normalised name.
    current = base.where("is_current").withColumn("_n", normalise_name(F.col("presentation_name")))
    unique_names = (current.groupBy("_n").agg(F.countDistinct("presentation_code").alias("_cnt"),
                                              F.first("presentation_code").alias("_successor"))
                           .where("_cnt = 1 AND _n != ''"))
    retired = base.where(~F.col("is_current")).withColumn("_n", normalise_name(F.col("presentation_name")))
    successor = retired.join(unique_names, "_n", "left").select(
        retired["presentation_code"].alias("_code"), F.col("_successor"))

    out = (base.join(successor, base["presentation_code"] == successor["_code"], "left")
               .withColumn("current_presentation_code",
                           F.when(F.col("is_current"), F.col("presentation_code")).otherwise(F.col("_successor")))
               .withColumn("lineage_method",
                           F.when(F.col("is_current"), F.lit("current"))
                            .when(F.col("_successor").isNotNull(), F.lit("same_name"))
                            .otherwise(F.lit("retired_no_successor")))
               .drop("_code", "_successor"))

    # Express history in today's groups: take the hierarchy from the successor when there is one.
    hier = [c for lvl in LEVELS for c in (f"{lvl}_code", f"{lvl}_name")] + ["presentation_name"]
    cur_h = base.where("is_current").select(F.col("presentation_code").alias("_cur"),
                                            *[F.col(c).alias(f"_c_{c}") for c in hier])
    out = out.join(cur_h, out["current_presentation_code"] == cur_h["_cur"], "left")
    for c in hier:
        out = out.withColumn(c, F.coalesce(F.col(f"_c_{c}"), F.col(c)))
    return out.select("presentation_code", "current_presentation_code", "lineage_method",
                      *hier, "first_seen_snapshot", "last_seen_snapshot", "is_current")


# ---- tables --------------------------------------------------------------

def silver_bnf(spark: SparkSession, cfg: Config) -> None:
    from .silver import latest_file_versions

    parts = []
    for kind in DATASETS:
        if spark.catalog.tableExists(cfg.bronze_bnf(kind)):
            parts.append(to_silver_bnf(latest_file_versions(spark.table(cfg.bronze_bnf(kind)))))
    if not parts:
        raise RuntimeError("No bronze BNF tables; run the bnf ingest and bronze first")
    snaps = parts[0]
    for extra in parts[1:]:
        snaps = snaps.unionByName(extra)      # by name: the two layouts differ in column order
    (snaps.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(cfg.silver_bnf_snapshot))
    s = spark.table(cfg.silver_bnf_snapshot)
    n_snaps = s.select("snapshot_key").distinct().count()
    print(f"silver_bnf_snapshot: {s.count():,} rows across {n_snaps} snapshots")

    dim = build_dim_bnf(s)
    (dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(cfg.dim_bnf))
    d = spark.table(cfg.dim_bnf)
    counts = {r["lineage_method"]: r["count"] for r in d.groupBy("lineage_method").count().collect()}
    print(f"dim_bnf: {d.count():,} presentations {counts}")
    if counts.get("current", 0) < 40_000:      # the BNF has ~55,000 presentations
        raise RuntimeError(f"dim_bnf has only {counts.get('current', 0):,} current presentations; partial load?")
    dup = d.groupBy("presentation_code").count().where("count > 1").count()
    if dup:
        raise RuntimeError(f"{dup} duplicate presentation codes in dim_bnf")


def bnf(spark: SparkSession, cfg: Config) -> None:
    """Landing -> bronze (per layout) -> silver_bnf_snapshot + dim_bnf."""
    ingest_bnf(cfg)
    for kind in DATASETS:   # one bronze table per layout, as with the EPD legacy/snomed files
        load_to_bronze(spark, cfg, name=f"bronze_bnf_{kind}", landing=cfg.landing(f"bnf_{kind}"),
                       table=cfg.bronze_bnf(kind))
    silver_bnf(spark, cfg)
