import os
import pathlib
import sys

import pytest

# Spark 4 needs Java 17/21; prefer a local Temurin 21 if present.
_TEMURIN = "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"
if "JAVA_HOME" not in os.environ and pathlib.Path(_TEMURIN).exists():
    os.environ["JAVA_HOME"] = _TEMURIN

# Workers must run the same Python as the driver (the venv), not the system one.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[1]")
               .appName("epd-tests")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
