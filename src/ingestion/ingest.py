"""Landing: download monthly files via the CKAN API.

Files go to _staging/ first and are moved into landing/ only when complete,
so Auto Loader never picks up a half-downloaded file.
"""
import os
import re
import shutil
import zipfile

import requests

from .config import CKAN, Config

MONTH_RE = re.compile(r"(20\d{2})(0[1-9]|1[0-2])")
CHUNK = 16 * 1024 * 1024


def list_month_resources(dataset_id: str) -> dict[str, str]:
    """Return {YYYYMM: url} for the monthly CSV/ZIP resources of a dataset."""
    r = requests.get(f"{CKAN}/package_show", params={"id": dataset_id}, timeout=60)
    r.raise_for_status()
    out = {}
    for res in r.json()["result"]["resources"]:
        fmt = (res.get("format") or "").upper()
        m = MONTH_RE.search(res.get("name") or "") or MONTH_RE.search(res.get("url") or "")
        if m and fmt in ("CSV", "ZIP"):
            if m.group(0) not in out or fmt == "CSV":  # prefer CSV over ZIP
                out[m.group(0)] = res["url"]
    return out


def latest_published_month(cfg: Config) -> str:
    """Newest month published across both datasets."""
    return max(
        month
        for dataset_id in cfg.datasets.values()
        for month in list_month_resources(dataset_id)
    )


def download_month(cfg: Config, src: str, month: str, url: str) -> str:
    landing = cfg.landing(src)
    if any(f.startswith(f"epd_{month}") for f in os.listdir(landing)):
        return "already landed"

    tmp = f"{cfg.vol}/_staging/{src}_{month}.part"
    with requests.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK):
                f.write(chunk)

    if zipfile.is_zipfile(tmp):
        with zipfile.ZipFile(tmp) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            for i, name in enumerate(csvs):
                part = f"{cfg.vol}/_staging/{src}_{month}_{i}.csv"
                with z.open(name) as zin, open(part, "wb") as zout:
                    shutil.copyfileobj(zin, zout, length=CHUNK)
                shutil.move(part, f"{landing}/epd_{month}_{i}.csv")
        os.remove(tmp)
    else:
        shutil.move(tmp, f"{landing}/epd_{month}.csv")
    return "downloaded"


def ingest(cfg: Config) -> None:
    """Download every month in range that isn't already landed.

    Raises if a requested month isn't published, so the job fails loudly
    instead of silently producing an empty month.
    """
    for d in ["_staging", "_checkpoints"] + [f"landing/{s}" for s in cfg.datasets]:
        os.makedirs(f"{cfg.vol}/{d}", exist_ok=True)

    published = {src: list_month_resources(ds) for src, ds in cfg.datasets.items()}
    missing = []
    for month in cfg.months():
        src = cfg.source_for(month)
        url = published[src].get(month)
        if url is None:
            missing.append(f"{src}:{month}")
            continue
        print(src, month, download_month(cfg, src, month, url))
    if missing:
        raise RuntimeError(f"Months not published on NHSBSA yet: {', '.join(missing)}")
