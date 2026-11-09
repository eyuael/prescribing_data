from datetime import date

import pytest

from ingestion.geography import (latest_onspd_release, pick_column, pick_latest_release,
                                 to_silver_imd, to_silver_onspd)
from ingestion.gold import with_practice_context


def item(name, type_="CSV Collection", id_="x"):
    return {"id": id_, "attributes": {"id": id_, "name": name, "type": type_}}


def test_pick_latest_release_ignores_hosted_tables_and_other_products():
    items = [item("ONS Postcode Directory (February 2026)", id_="feb"),
             item("ONS Postcode Directory (May 2026)", id_="may"),
             item("ONS Postcode Directory (August 2026) for the UK (Hosted Table)", "Table", "bad"),
             item("ONS Postcode Directory (November 2025)", id_="nov25"),
             item("NHS Postcode Directory (May 2026)", id_="nhs")]
    assert pick_latest_release(items) == {"item_id": "may", "name": "ONS Postcode Directory (May 2026)",
                                          "release": "MAY_2026"}
    assert pick_latest_release([]) is None


def test_pick_column_takes_newest_year():
    assert pick_column(["lad24cd", "lad25cd", "lsoa21cd"], r"lad\d{2}cd") == "lad25cd"
    assert pick_column(["x"], r"lad\d{2}cd") is None


ONSPD = ("pcds string, dointr string, doterm string, lad25cd string, icb26cd string, lsoa21cd string, "
         "_source_file string")


def test_onspd_latest_release_and_silver(spark):
    df = spark.createDataFrame([
        ("TS18 1HU", "199002", "", "E06000004", "QHM", "E01012000", "/v/ONSPD_FEB_2026_UK_TS.csv"),
        ("TS18 1HU", "199002", "", "E06000004", "QHM", "E01099999", "/v/ONSPD_MAY_2026_UK_TS.csv"),
        ("ts18  2aw", "199002", "202001", "E06000004", "QHM", "E01012001", "/v/ONSPD_MAY_2026_UK_TS.csv"),
        ("ZZ1 1ZZ", "199002", "", "S12000001", "", "", "/v/ONSPD_MAY_2026_UK_ZZ.csv"),  # no LSOA
    ], ONSPD)
    rows = {r.postcode_key: r for r in to_silver_onspd(latest_onspd_release(df)).collect()}
    assert set(rows) == {"TS181HU", "TS182AW"}          # Feb dropped, no-LSOA dropped, spaces normalised
    assert rows["TS181HU"].lsoa21_code == "E01099999"   # newest release wins
    assert rows["TS181HU"].onspd_release == "202605"
    assert rows["TS182AW"].date_terminated == date(2020, 1, 1)
    assert rows["TS181HU"].lad_code == "E06000004" and rows["TS181HU"].icb_code == "QHM"


def test_onspd_silver_survives_year_suffix_drift(spark):
    df = spark.createDataFrame([("TS18 1HU", "", "", "E06000004", "QHM", "E01012000", "/v/ONSPD_MAY_2026_UK_TS.csv")],
                               ONSPD).withColumnRenamed("lad25cd", "lad26cd").withColumnRenamed("icb26cd", "icb27cd")
    r = to_silver_onspd(latest_onspd_release(df)).first()
    assert r.lad_code == "E06000004" and r.icb_code == "QHM"


def test_onspd_silver_fails_loudly_without_lsoa(spark):
    df = spark.createDataFrame([("TS18 1HU", "/v/ONSPD_MAY_2026_UK_TS.csv")], "pcds string, _source_file string")
    with pytest.raises(RuntimeError, match="lsoa21cd"):
        to_silver_onspd(latest_onspd_release(df))


IMD_HEADERS = ["LSOA code (2021)", "LSOA name (2021)", "Local Authority District code (2024)",
               "Local Authority District name (2024)", "Index of Multiple Deprivation (IMD) Score",
               "Index of Multiple Deprivation (IMD) Rank (where 1 is most deprived)",
               "Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)",
               "Income Decile (where 1 is most deprived 10% of LSOAs)",
               "Income Deprivation Affecting Children Index (IDACI) Decile (where 1 is most deprived)",
               "_source_file"]


def test_imd_silver_maps_long_headers(spark):
    df = spark.createDataFrame([("e01000001", "City 001A", "e09000001", "City", "12.5", "30000", "9",
                                 "8", "1", "/v/imd_2025.csv")], IMD_HEADERS)
    r = to_silver_imd(df).first()
    assert (r.lsoa21_code, r.imd_score, r.imd_rank, r.imd_decile) == ("E01000001", 12.5, 30000, 9)
    assert r.income_decile == 8          # not confused with the IDACI decile
    assert r.crime_decile is None        # optional domain absent in this fixture


def test_imd_silver_fails_loudly_when_core_column_renamed(spark):
    df = spark.createDataFrame([("e01", "x")], ["LSOA code (2021)", "_source_file"])
    with pytest.raises(RuntimeError, match="layout changed"):
        to_silver_imd(df)


def test_with_practice_context_asof_join_and_row_count(spark):
    jan, feb = date(2025, 1, 1), date(2025, 2, 1)
    pm = spark.createDataFrame(
        [(jan, "A1", "ts18 1hu"), (feb, "A1", "ts18 1hu"), (jan, "B1", None), (jan, "C1", "ZZ9 9ZZ")],
        "year_month date, practice_code string, postcode string")
    dim = spark.createDataFrame(
        [("A1", "Active", "TS18 1HU", date(1900, 1, 1), date(2025, 2, 1)),
         ("A1", "Inactive", "TS18 1HU", date(2025, 2, 1), None),
         ("B1", "Active", "BB1 1BB", date(1900, 1, 1), None)],
        "practice_code string, status string, postcode string, effective_from date, effective_to date")
    onspd = spark.createDataFrame(
        [("TS181HU", "E01000001", "E06000004", "QHM"), ("BB11BB", "E01000002", "E06000008", "QE1")],
        "postcode_key string, lsoa21_code string, lad_code string, icb_code string")
    imd = spark.createDataFrame([("E01000001", 20.0, 100, 3), ("E01000002", 5.0, 30000, 9)],
                                "lsoa21_code string, imd_score double, imd_rank int, imd_decile int")
    out = with_practice_context(pm, dim, onspd, imd)
    assert out.count() == 4                                    # joins never add or drop rows
    rows = {(r.practice_code, r.year_month): r for r in out.collect()}
    assert rows[("A1", jan)].ods_status == "Active"            # as-of the month, not today
    assert rows[("A1", feb)].ods_status == "Inactive"
    assert rows[("A1", jan)].imd_decile == 3 and rows[("A1", jan)].lad_code == "E06000004"
    assert rows[("B1", jan)].lsoa21_code == "E01000002"        # EPD postcode null -> ODS postcode
    assert rows[("C1", jan)].lsoa21_code is None and rows[("C1", jan)].ods_status is None


def test_slug_makes_delta_safe_names():
    from ingestion.autoloader import slug
    out = slug("Income Decile (where 1 is most deprived 10% of LSOAs)")
    assert out == "income_decile_where_1_is_most_deprived_10_of_lsoas"
    assert not set(" ,;{}()\n\t=") & set(out)
    assert slug("LSOA code (2021)") == "lsoa_code_2021"


def test_imd_silver_accepts_slugged_bronze_columns(spark):
    from ingestion.autoloader import slug
    cols = [slug(c) if not c.startswith("_") else c for c in IMD_HEADERS]
    df = spark.createDataFrame([("e01000001", "City 001A", "e09000001", "City", "12.5", "30000", "9",
                                 "8", "1", "/v/imd_2025.csv")], cols)
    r = to_silver_imd(df).first()
    assert (r.lsoa21_code, r.imd_decile, r.income_decile) == ("E01000001", 9, 8)
