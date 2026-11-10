from datetime import date

from ingestion.gold import with_list_size
from ingestion.list_size import find_zip_url, publication_url, to_silver_list_size

COLS = ("publication string, extract_date string, type string, sub_icb_location_code string, "
        "ons_sub_icb_location_code string, code string, postcode string, sex string, age string, "
        "number_of_patients string, _source_file string, _ingested_at timestamp")


def raw(spark):
    return spark.createDataFrame([
        ("P", "2026-10-01", "GP", "16C", "E1", " a81001 ", "ts18 1hu", "ALL", "ALL", "3684", "f", None),
        ("P", "2026-10-01", "GP", "16C", "E1", "A81002", "TS18 2AW", "ALL", "ALL", "x", "f", None),
        ("P", "2026-10-01", "GP", "16C", "E1", "A81001", "TS18 1HU", "FEMALE", "ALL", "10", "f", None),
        ("P", "2026-10-01", "ICB", "16C", "E1", "QHM", None, "ALL", "ALL", "999", "f", None),
    ], COLS)


def test_silver_keeps_only_practice_all_persons_rows(spark):
    rows = {r.practice_code: r for r in to_silver_list_size(raw(spark)).collect()}
    assert set(rows) == {"A81001", "A81002"}
    assert rows["A81001"].patients == 3684
    assert rows["A81001"].postcode == "TS18 1HU"
    assert rows["A81001"].year_month == date(2026, 10, 1)
    assert rows["A81002"].patients is None  # unparseable -> null, not an error


def test_publication_url_uses_lowercase_month_name():
    assert publication_url("202610").endswith("/patients-registered-at-a-gp-practice/october-2026")


def test_find_zip_url_ignores_other_files():
    html = ('<a href="https://files.digital.nhs.uk/1C/E17AD5/gp-reg-pat-prac-sing-age-regions.zip">x</a>'
            '<a href="https://files.digital.nhs.uk/FB/7851F6/gp-reg-pat-prac-all.zip">y</a>')
    assert find_zip_url(html) == "https://files.digital.nhs.uk/FB/7851F6/gp-reg-pat-prac-all.zip"
    assert find_zip_url("<html></html>") is None


def test_with_list_size_rates_and_missing_lists(spark):
    m = date(2025, 1, 1)
    pm = spark.createDataFrame(
        [(m, "A1", 500, 2000.0), (m, "B1", 10, 50.0), (m, "C1", 10, 50.0)],
        "year_month date, practice_code string, items bigint, actual_cost double")
    ls = spark.createDataFrame(
        [(m, "A1", 1000), (m, "C1", 0)],
        "year_month date, practice_code string, patients bigint")
    rows = {r.practice_code: r for r in with_list_size(pm, ls).collect()}
    assert rows["A1"].items_per_1000 == 500.0
    assert rows["A1"].cost_per_1000 == 2000.0
    assert rows["B1"].list_size is None and rows["B1"].items_per_1000 is None  # row kept
    assert rows["C1"].items_per_1000 is None  # zero list size -> null


def test_find_zip_url_accepts_bare_csv_months():
    html = '<a href="https://files.digital.nhs.uk/0D/5048F1/gp-reg-pat-prac-all.csv">x</a>'
    assert find_zip_url(html) == "https://files.digital.nhs.uk/0D/5048F1/gp-reg-pat-prac-all.csv"
