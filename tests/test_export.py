from datetime import date

from ingestion.export import EXPORT_COLUMNS, build_manifest, practice_month_export


def test_export_selects_only_chart_columns_in_stable_order(spark):
    m = date(2025, 1, 1)
    gold = spark.createDataFrame(
        [(date(2025, 2, 1), "B1", "Practice B", 9, 5, 20.0, 3, 1.0),
         (m, "B1", "Practice B", 9, 5, 10.0, 2, 1.0),
         (m, "A1", "Practice A", 3, 7, 30.0, 4, 1.0)],
        "year_month date, practice_code string, practice_name string, imd_decile int, "
        "list_size int, actual_cost double, items int, nic double")
    out = practice_month_export(gold)
    assert out.columns == EXPORT_COLUMNS
    assert "practice_name" not in out.columns            # names live in dimensions, not facts
    assert [(r.year_month, r.practice_code) for r in out.collect()] == [
        (m, "A1"), (m, "B1"), (date(2025, 2, 1), "B1")]


def test_manifest_lists_months_and_the_file():
    m = build_manifest(["2025-01", "2025-02"], 120, "2026-11-15T10:00:00+00:00")
    assert m["months"] == ["2025-01", "2025-02"] and m["rows"] == 120
    assert m["files"]["practice_month"] == "practice_month.parquet"
