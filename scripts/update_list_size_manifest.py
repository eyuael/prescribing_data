"""Add GP list-size zip URLs to src/ingestion/list_size_urls.json.

Run this on your own machine, not on Databricks: digital.nhs.uk's Cloudflare
challenge blocks Databricks IP ranges (HTTP 403) but not a normal connection.
Then commit the JSON and `databricks bundle deploy`, so the job reads the URL
from the manifest and never has to touch the blocked page.

    .venv/bin/python scripts/update_list_size_manifest.py 202501 202610
"""
import json
import pathlib
import sys
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
from ingestion.config import Config  # noqa: E402
from ingestion.list_size import HEADERS, MANIFEST, find_zip_url, publication_url  # noqa: E402


def main(start: str, end: str) -> None:
    urls = json.loads(MANIFEST.read_text())
    for month in Config(start, end).months():
        if month in urls:
            print(month, "already in manifest")
            continue
        try:
            req = urllib.request.Request(publication_url(month), headers=HEADERS)
            html = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            print(month, f"page HTTP {e.code} (not published yet, or blocked here too)")
            continue
        url = find_zip_url(html)
        print(month, url or "page found but no all-persons zip link")
        if url:
            urls[month] = url
    MANIFEST.write_text(json.dumps(dict(sorted(urls.items())), indent=2) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
