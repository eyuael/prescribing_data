"""Databricks job entry point (spark_python_task).

The bundle syncs the repo to the workspace, so `src/` sits next to `jobs/`.
Arguments are passed straight through to ingestion.pipeline, e.g.
    run_monthly.py --stages silver --catalog workspace --schema epd
"""
import inspect
import pathlib
import sys

# Serverless runs this file via exec(compile(...)), so `__file__` is undefined.
# The path given to compile() is still available on the current frame.
_this_file = pathlib.Path(inspect.currentframe().f_code.co_filename).resolve()
sys.path.insert(0, str(_this_file.parent.parent / "src"))

from ingestion.pipeline import main  # noqa: E402

if __name__ == "__main__":
    main()
