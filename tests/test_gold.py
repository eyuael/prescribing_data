from datetime import date

from ingestion.gold import bnf_chapter_month, practice_month

SCHEMA = ("year_month date, practice_code string, practice_name string, postcode string, "
          "icb_code string, bnf_code string, items bigint, actual_cost double, nic double")
M = date(2025, 1, 1)


def silver(spark):
    return spark.createDataFrame([
        (M, "A1", "Practice A", "AA1 1AA", "Q1", "0101aaa", 10, 100.0, 90.0),
        (M, "A1", "Practice A", "AA1 1AA", "Q1", "0102bbb", 5, 50.0, 45.0),
        (M, "B1", "Practice B", "BB1 1BB", "Q2", "0201ccc", 7, 70.0, 63.0),
    ], SCHEMA)


def test_practice_month_aggregates(spark):
    rows = {r.practice_code: r for r in practice_month(silver(spark)).collect()}
    assert rows["A1"].items == 15
    assert rows["A1"].actual_cost == 150.0
    assert rows["A1"].distinct_products == 2
    assert rows["B1"].distinct_products == 1


def test_practice_month_is_one_row_per_practice_month(spark):
    df = practice_month(silver(spark))
    assert df.count() == df.select("year_month", "practice_code").distinct().count() == 2


def test_bnf_chapter_month_groups_on_first_two_chars(spark):
    rows = {r.bnf_chapter: r for r in bnf_chapter_month(silver(spark)).collect()}
    assert set(rows) == {"01", "02"}
    assert rows["01"].items == 15
    assert rows["02"].actual_cost == 70.0
