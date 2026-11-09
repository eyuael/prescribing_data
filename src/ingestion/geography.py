"""Geography and deprivation: ONS Postcode Directory (ONSPD) + English IoD 2025.

Join path: practice postcode -> LSOA 2021 (ONSPD) -> deprivation (IoD 2025).
IoD 2025 is built on LSOA 2021, matching ONSPD's `lsoa21cd`. (IoD 2019 used
LSOA 2011; mixing the two would silently drop or mis-match areas.)

ONSPD: ~250 MB zip, released quarterly, no stable URL. We find the newest
"ONS Postcode Directory (<Month> <Year>)" via the ONS Open Geography portal
(ArcGIS Hub) search API, then download the item. Set ONSPD_ITEM_ID to pin one.
The column names carry a year (`lad25cd`, `icb26cd`) and change between
releases, so silver picks them by pattern instead of by exact name.

IoD 2025 File 7 (all ranks, scores, deciles) is a stable gov.uk asset.

Both are reference snapshots: silver_onspd/silver_imd hold the newest release
only. A practice's LSOA is therefore "as of the latest ONSPD", not as of the
prescribing month. That is fine for deprivation (LSOAs rarely move) and is
noted rather than modelled.
"""
import os
import re
import shutil
import zipfile

from pyspark.sql import DataFrame, SparkSession, functions as F

from .autoloader import load_to_bronze
from .config import Config

HUB_SEARCH = "https://open-geography-portalx-ons.hub.arcgis.com/api/v3/datasets"
ITEM_DATA = "https://www.arcgis.com/sharing/rest/content/items/{item_id}/data"
IMD_URL = ("https://assets.publishing.service.gov.uk/media/691ded56d140bbbaa59a2a7d/"
           "File_7_IoD2025_All_Ranks_Scores_Deciles_Population_Denominators.csv")
IMD_RELEASE = "2025"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; epd-pipeline)"}

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
NAME_RE = re.compile(r"^ONS Postcode Directory \((\w+) (\d{4})\)$")


# ---- landing -------------------------------------------------------------

def pick_latest_release(items: list[dict]) -> dict | None:
    """Newest plain ONSPD release among Hub search results.

    Filters out the hosted-table copies, user guides and other postcode
    products that the search also returns: name must match exactly and the
    item must be the zipped CSV collection.
    """
    best = None
    for it in items:
        a = it.get("attributes", it)
        m = NAME_RE.match(a.get("name", ""))
        if not m or a.get("type") != "CSV Collection" or m.group(1)[:3].upper() not in MONTHS:
            continue
        key = (int(m.group(2)), MONTHS.index(m.group(1)[:3].upper()))
        if best is None or key > best[0]:
            best = (key, {"item_id": a.get("id") or it.get("id"), "name": a["name"],
                          "release": f"{m.group(1)[:3].upper()}_{m.group(2)}"})
    return best[1] if best else None


def find_onspd_release() -> dict:
    import requests

    if os.environ.get("ONSPD_ITEM_ID"):
        return {"item_id": os.environ["ONSPD_ITEM_ID"], "name": "pinned", "release": None}
    r = requests.get(HUB_SEARCH, headers=HEADERS, timeout=60, params={
        "q": "ONS Postcode Directory", "page[size]": 50,
        "fields[datasets]": "id,name,type,size"})
    r.raise_for_status()
    found = pick_latest_release(r.json()["data"])
    if not found:
        raise RuntimeError("No ONSPD release found via the ONS Open Geography search. "
                           "Set ONSPD_ITEM_ID or upload the CSVs to landing/onspd/ manually.")
    return found


def ingest_onspd(cfg: Config) -> None:
    import requests

    landing = cfg.landing("onspd")
    os.makedirs(landing, exist_ok=True)
    os.makedirs(f"{cfg.vol}/_staging", exist_ok=True)
    rel = find_onspd_release()
    if rel["release"] and any(f.startswith(f"ONSPD_{rel['release']}_") for f in os.listdir(landing)):
        print("onspd", rel["name"], "already landed")
        return

    tmp = f"{cfg.vol}/_staging/onspd.zip"
    with requests.get(ITEM_DATA.format(item_id=rel["item_id"]), headers=HEADERS,
                      stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=16 * 1024 * 1024):
                f.write(chunk)

    # The zip holds one 1.4 GB CSV plus the same data split by postcode area.
    # Land the split files: smaller, parallel-friendly, and the file name carries the release.
    with zipfile.ZipFile(tmp) as z:
        parts = [n for n in z.namelist() if "/multi_csv/" in n and n.lower().endswith(".csv")]
        if not parts:
            raise RuntimeError("ONSPD zip has no multi_csv/ files; layout changed")
        stage = f"{cfg.vol}/_staging/onspd"
        shutil.rmtree(stage, ignore_errors=True)
        os.makedirs(stage)
        for n in parts:
            out = f"{stage}/{os.path.basename(n)}"
            with z.open(n) as zin, open(out, "wb") as zout:
                shutil.copyfileobj(zin, zout, length=16 * 1024 * 1024)
    os.remove(tmp)
    for f in os.listdir(stage):          # move in only once everything is extracted
        shutil.move(f"{stage}/{f}", f"{landing}/{f}")
    print(f"onspd {rel['name']}: landed {len(parts)} files")


def ingest_imd(cfg: Config) -> None:
    import requests

    landing = cfg.landing("imd")
    os.makedirs(landing, exist_ok=True)
    os.makedirs(f"{cfg.vol}/_staging", exist_ok=True)
    target = f"{landing}/imd_{IMD_RELEASE}.csv"
    if os.path.exists(target):
        print("imd", IMD_RELEASE, "already landed")
        return
    tmp = f"{cfg.vol}/_staging/imd_{IMD_RELEASE}.csv"
    with requests.get(IMD_URL, headers=HEADERS, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    shutil.move(tmp, target)
    print("imd", IMD_RELEASE, "downloaded")


# ---- silver (pure DataFrame functions) -----------------------------------

def pick_column(columns: list[str], pattern: str) -> str | None:
    """Column matching `pattern`; if several (lad24cd, lad25cd) the one with the highest year."""
    hits = [c for c in columns if re.fullmatch(pattern, c)]
    return max(hits, key=lambda c: re.findall(r"\d+", c)[-1:] or ["0"]) if hits else None


def normalise_postcode(col):
    """Uppercase and strip all whitespace so 'ts18 1hu' and 'TS18  1HU' match."""
    return F.upper(F.regexp_replace(col, r"\s+", ""))


def latest_onspd_release(df: DataFrame) -> DataFrame:
    """Keep only rows from the newest ONSPD release (from file names like ONSPD_MAY_2026_UK_AB.csv)."""
    mon = F.upper(F.regexp_extract("_source_file", r"ONSPD_([A-Za-z]{3})[A-Za-z]*_(\d{4})_", 1))
    yr = F.regexp_extract("_source_file", r"ONSPD_([A-Za-z]{3})[A-Za-z]*_(\d{4})_", 2)
    month_num = F.array_position(F.array(*[F.lit(m) for m in MONTHS]), mon)
    d = df.withColumn("_rel", F.when(yr != "", F.concat(yr, F.lpad(month_num.cast("string"), 2, "0"))))
    d = d.where(F.col("_rel").isNotNull())
    newest = d.agg(F.max("_rel")).first()[0]
    return d.where(F.col("_rel") == newest).withColumnRenamed("_rel", "onspd_release")


def to_silver_onspd(df: DataFrame) -> DataFrame:
    """Postcode -> LSOA 2021 / local authority / ICB lookup, one row per postcode."""
    df = df.toDF(*[c.strip().lower() if not c.startswith("_") else c for c in df.columns])
    cols = df.columns
    lsoa = pick_column(cols, r"lsoa21cd")
    pcds = pick_column(cols, r"pcds")
    if lsoa is None or pcds is None:
        raise RuntimeError(f"ONSPD is missing lsoa21cd/pcds (have {cols[:10]}...); check release layout")
    lad = pick_column(cols, r"lad\d{2}cd")
    icb = pick_column(cols, r"icb\d{2}cd")

    def text(c):
        if c is None:
            return F.lit(None).cast("string")
        t = F.trim(F.col(c))
        return F.when(t == "", None).otherwise(t)

    def ym(c):
        return F.expr(f"try_to_date(trim(`{c}`), 'yyyyMM')") if c in cols else F.lit(None).cast("date")

    return (df.select(
                normalise_postcode(F.trim(F.col(pcds))).alias("postcode_key"),
                F.upper(F.trim(F.col(pcds))).alias("postcode"),
                F.upper(text(lsoa)).alias("lsoa21_code"),
                F.upper(text(lad)).alias("lad_code"),
                F.upper(text(icb)).alias("icb_code"),
                ym("dointr").alias("date_introduced"),
                ym("doterm").alias("date_terminated"),
                F.col("onspd_release"))
            .where(F.col("lsoa21_code").isNotNull())
            .dropDuplicates(["postcode_key"]))


# IoD column names are long sentences; match by prefix and map to short names.
IMD_COLUMNS = {
    "lsoa21_code": ("lsoa code (2021)", "string"),
    "lsoa21_name": ("lsoa name (2021)", "string"),
    "lad_code": ("local authority district code", "string"),
    "lad_name": ("local authority district name", "string"),
    "imd_score": ("index of multiple deprivation (imd) score", "double"),
    "imd_rank": ("index of multiple deprivation (imd) rank", "int"),
    "imd_decile": ("index of multiple deprivation (imd) decile", "int"),
    "income_decile": ("income decile", "int"),
    "employment_decile": ("employment decile", "int"),
    "education_decile": ("education, skills and training decile", "int"),
    "health_decile": ("health deprivation and disability decile", "int"),
    "crime_decile": ("crime decile", "int"),
    "housing_decile": ("barriers to housing and services decile", "int"),
    "living_env_decile": ("living environment decile", "int"),
}


def to_silver_imd(df: DataFrame) -> DataFrame:
    by_lower = {c.strip().lower(): c for c in df.columns}
    out = []
    for name, (prefix, dtype) in IMD_COLUMNS.items():
        src = next((orig for low, orig in by_lower.items() if low.startswith(prefix)), None)
        if src is None:
            if name in ("lsoa21_code", "imd_score", "imd_decile"):
                raise RuntimeError(f"IoD file has no column starting '{prefix}'; layout changed")
            out.append(F.lit(None).cast(dtype).alias(name))   # optional domain: null, flagged by tests
        elif dtype == "string":
            out.append(F.upper(F.trim(F.col(f"`{src}`"))).alias(name)
                       if name.endswith("_code") else F.trim(F.col(f"`{src}`")).alias(name))
        else:
            out.append(F.expr(f"try_cast(trim(`{src}`) AS {dtype})").alias(name))
    return (df.select(*out, F.col("_source_file"))
              .where(F.col("lsoa21_code").isNotNull())
              .dropDuplicates(["lsoa21_code"]))


# ---- silver (tables) -----------------------------------------------------

def silver_onspd(spark: SparkSession, cfg: Config) -> None:
    from .silver import latest_file_versions

    if not spark.catalog.tableExists(cfg.bronze_onspd):
        raise RuntimeError(f"{cfg.bronze_onspd} missing; run the geography ingest and bronze first")
    raw = latest_onspd_release(latest_file_versions(spark.table(cfg.bronze_onspd)))
    df = to_silver_onspd(raw)
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(cfg.silver_onspd))
    row = spark.table(cfg.silver_onspd).agg(F.count("*").alias("n"), F.max("onspd_release").alias("r")).first()
    print(f"silver_onspd: release {row['r']}, {row['n']:,} postcodes")
    if row["n"] < 1_000_000:   # whole UK is ~2.7M, England alone ~1.7M
        raise RuntimeError(f"silver_onspd only has {row['n']:,} postcodes; a partial load?")


def silver_imd(spark: SparkSession, cfg: Config) -> None:
    from .silver import latest_file_versions

    if not spark.catalog.tableExists(cfg.bronze_imd):
        raise RuntimeError(f"{cfg.bronze_imd} missing; run the geography ingest and bronze first")
    raw = latest_file_versions(spark.table(cfg.bronze_imd))
    release = F.regexp_extract("_source_file", r"imd_(\d{4})", 1)
    newest = raw.agg(F.max(release)).first()[0]
    df = to_silver_imd(raw.where(release == newest)).withColumn("imd_release", F.lit(newest))
    (df.drop("_source_file").write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(cfg.silver_imd))
    n = spark.table(cfg.silver_imd).count()
    print(f"silver_imd: release {newest}, {n:,} LSOAs")
    if n < 30_000:   # England has 33,755 LSOAs
        raise RuntimeError(f"silver_imd only has {n:,} LSOAs; expected ~33,755")


def geography(spark: SparkSession, cfg: Config) -> None:
    """Landing -> bronze -> silver for ONSPD and IoD."""
    ingest_onspd(cfg)
    ingest_imd(cfg)
    load_to_bronze(spark, cfg, name="bronze_onspd", landing=cfg.landing("onspd"), table=cfg.bronze_onspd)
    load_to_bronze(spark, cfg, name="bronze_imd", landing=cfg.landing("imd"), table=cfg.bronze_imd)
    silver_onspd(spark, cfg)
    silver_imd(spark, cfg)
