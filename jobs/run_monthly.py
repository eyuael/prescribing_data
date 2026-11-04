"""Databricks job entry point (spark_python_task).

The bundle syncs the repo to the workspace, so `src/` sits next to `jobs/`.
Arguments are passed straight through to ingestion.pipeline, e.g.
    run_monthly.py --stages silver --catalog workspace --schema epd
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from ingestion.pipeline import main  # noqa: E402

if __name__ == "__main__":
    main()
