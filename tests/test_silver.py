import re
from datetime import date, datetime

from ingestion.silver import DDL, latest_file_versions, to_silver

TS = datetime(2025, 1, 1)


def bronze_df(spark, rows, columns):
    """Bronze is all strings plus the metadata columns."""
    return spark.createDataFrame(rows, columns)


def one(spark, **overrides):
    row = {
        "YEAR_MONTH": "202501", "PRACTICE_CODE": " a81001 ", "POSTCODE": " ts1 1ab ",
        "BNF_CODE": "0101010c0aaaaaa", "ITEMS": "12", "ACTUAL_COST": "34.5", "NIC": "30.0",
        "_source_file": "epd_202501.csv", "_ingested_at": TS,
    }
    row.update(overrides)
    df = spark.createDataFrame([tuple(row.values())], list(row.keys()))
    return to_silver(df, "legacy").first()


def test_trims_uppercases_codes_and_postcodes(spark):
    r = one(spark)
    assert r.practice_code == "A81001"
    assert r.postcode == "TS1 1AB"
    assert r.bnf_code == "0101010C0AAAAAA"


def test_empty_string_becomes_null(spark):
    assert one(spark, POSTCODE="   ").postcode is None


def test_unparseable_number_becomes_null(spark):
    r = one(spark, ITEMS="n/a")
    assert r.items is None
    assert r.actual_cost == 34.5


def test_year_month_formats(spark):
    assert one(spark, YEAR_MONTH="202501").year_month == date(2025, 1, 1)
    assert one(spark, YEAR_MONTH="2025-08").year_month == date(2025, 8, 1)
    assert one(spark, YEAR_MONTH="2025-08-01").year_month == date(2025, 8, 1)


def test_bad_year_month_is_dropped_not_fatal(spark):
    df = bronze_df(spark, [("garbage", "A1", TS, "f"), ("202501", "A2", TS, "f")],
                   ["year_month", "practice_code", "_ingested_at", "_source_file"])
    out = to_silver(df, "legacy").collect()
    assert [r.practice_code for r in out] == ["A2"]


def test_stp_columns_map_to_icb(spark):
    r = one(spark, STP_CODE="qhm", STP_NAME="Some STP")
    assert r.icb_code == "QHM"
    assert r.icb_name == "Some STP"


def test_icb_preferred_over_stp(spark):
    r = one(spark, ICB_CODE="qux", STP_CODE="qhm")
    assert r.icb_code == "QUX"


def test_missing_source_column_is_null_not_error(spark):
    r = one(spark)  # fixture has no ADQUSAGE / PCO_CODE columns
    assert r.adqusage is None
    assert r.pco_code is None


def test_source_is_recorded(spark):
    assert one(spark).source == "legacy"


def test_output_columns_match_ddl(spark):
    """Silver DDL and to_silver() must stay in lockstep (CLAUDE.md convention)."""
    body = DDL.split("(", 1)[1].split("\n)", 1)[0]
    ddl_cols = [col.split()[0] for col in re.split(r",\s*", body) if col.strip()]
    df = spark.createDataFrame([("202501", "A1", TS, "f")],
                               ["year_month", "practice_code", "_ingested_at", "_source_file"])
    assert to_silver(df, "legacy").columns == ddl_cols


def test_latest_file_versions_drops_older_version(spark):
    old, new = datetime(2025, 2, 1), datetime(2025, 3, 1)
    df = spark.createDataFrame(
        [("f1", old, "old"), ("f1", new, "new"), ("f2", old, "other")],
        ["_source_file", "_file_modified_at", "tag"])
    tags = sorted(r.tag for r in latest_file_versions(df).collect())
    assert tags == ["new", "other"]


def test_latest_file_versions_keeps_legacy_null_rows_when_no_newer(spark):
    df = spark.createDataFrame(
        [("f1", None, "legacy")], "_source_file string, _file_modified_at timestamp, tag string")
    assert [r.tag for r in latest_file_versions(df).collect()] == ["legacy"]


def test_latest_file_versions_drops_null_rows_once_newer_exists(spark):
    df = spark.createDataFrame(
        [("f1", None, "legacy"), ("f1", datetime(2025, 3, 1), "new")],
        "_source_file string, _file_modified_at timestamp, tag string")
    assert [r.tag for r in latest_file_versions(df).collect()] == ["new"]


def test_latest_file_versions_noop_without_column(spark):
    df = spark.createDataFrame([("f1", "x")], ["_source_file", "tag"])
    assert latest_file_versions(df).count() == 1
