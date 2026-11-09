import json
from datetime import date

from ingestion.ods import (latest_snapshot, parse_detail, scd2_stage, to_silver_ods,
                           to_succession, with_row_hash)

RAW = ("org_id string, name string, status string, postcode string, last_change_date string, "
       "primary_role_id string, operational_start string, operational_end string, succs_json string")


def test_parse_detail_extracts_dates_and_successions():
    detail = {"Organisation": {
        "Date": [{"Type": "Operational", "Start": "1974-04-01", "End": "2020-03-31"}],
        "Succs": {"Succ": [{"Type": "Predecessor", "Date": [{"Type": "Legal", "Start": "2013-04-01"}],
                            "Target": {"OrgId": {"extension": "T1430"}, "PrimaryRoleId": {"id": "RO189"}}}]}}}
    out = parse_detail(detail)
    assert out["operational_start"] == "1974-04-01" and out["operational_end"] == "2020-03-31"
    assert json.loads(out["succs_json"]) == [
        {"type": "Predecessor", "target": "T1430", "target_role": "RO189", "date": "2013-04-01"}]


def test_parse_detail_with_no_successions_or_end_date():
    out = parse_detail({"Organisation": {"Date": [{"Type": "Operational", "Start": "2000-01-01"}]}})
    assert out["operational_end"] is None and json.loads(out["succs_json"]) == []


def snapshot(spark, rows):
    return to_silver_ods(spark.createDataFrame(rows, RAW))


def test_to_silver_ods_cleans_and_types(spark):
    r = snapshot(spark, [(" a81001 ", " THE SURGERY ", "Active", "ts18 1hu", "2023-08-22", "RO177",
                          "1974-04-01", "", "[]")]).first()
    assert r.practice_code == "A81001" and r.postcode == "TS18 1HU"
    assert r.operational_start == date(1974, 4, 1) and r.operational_end is None


def dim(spark, rows):
    return spark.createDataFrame(rows, "practice_code string, row_hash string, is_current boolean")


def test_scd2_stage_new_changed_and_unchanged(spark):
    inc = with_row_hash(snapshot(spark, [
        ("A1", "Same", "Active", "AA1 1AA", None, "RO177", "2000-01-01", None, "[]"),
        ("B1", "Renamed", "Active", "BB1 1BB", None, "RO177", "2000-01-01", None, "[]"),
        ("C1", "Brand new", "Active", "CC1 1CC", None, "RO177", "2000-01-01", None, "[]"),
    ])).drop("last_change_date", "succs_json")
    hashes = {r.practice_code: r.row_hash for r in inc.collect()}
    cur = dim(spark, [("A1", hashes["A1"], True), ("B1", "old-hash", True), ("D1", "x", True)])
    staged = scd2_stage(inc, cur, "2026-11-08").collect()

    codes = sorted((r.practice_code, r.merge_key is None) for r in staged)
    # B1 (changed) and C1 (new) each appear as a close row and an insert row; A1 and D1 do not
    assert codes == [("B1", False), ("B1", True), ("C1", False), ("C1", True)]
    assert all(r.effective_from == date(2026, 11, 8) for r in staged)


def test_scd2_stage_ignores_closed_versions_when_comparing(spark):
    inc = with_row_hash(snapshot(spark, [
        ("A1", "Name", "Active", "AA1 1AA", None, "RO177", None, None, "[]")])).drop("last_change_date", "succs_json")
    h = inc.first().row_hash
    cur = dim(spark, [("A1", h, False)])   # only an old, closed version exists
    assert scd2_stage(inc, cur, "2026-11-08").count() == 2


def test_scd2_stage_is_a_noop_when_nothing_changed(spark):
    inc = with_row_hash(snapshot(spark, [
        ("A1", "Name", "Active", "AA1 1AA", None, "RO177", None, None, "[]")])).drop("last_change_date", "succs_json")
    cur = dim(spark, [("A1", inc.first().row_hash, True)])
    assert scd2_stage(inc, cur, "2026-11-08").count() == 0


def test_to_succession_normalises_both_directions(spark):
    # Closed practice lists a Successor; the surviving one lists the same pair as a Predecessor.
    closed = json.dumps([{"type": "Successor", "target": "NEW1", "target_role": "RO177", "date": "2024-04-01"}])
    survivor = json.dumps([{"type": "Predecessor", "target": "OLD1", "target_role": "RO177", "date": "2024-04-01"}])
    df = snapshot(spark, [
        ("OLD1", "Old", "Inactive", None, None, "RO177", None, None, closed),
        ("NEW1", "New", "Active", None, None, "RO177", None, None, survivor)])
    rows = to_succession(df).collect()
    assert len(rows) == 1
    assert (rows[0].predecessor_code, rows[0].successor_code) == ("OLD1", "NEW1")
    assert rows[0].succession_date == date(2024, 4, 1)


def test_latest_snapshot_picks_newest_file(spark):
    df = spark.createDataFrame([("a", "/v/ods/ods_20261001.jsonl"), ("b", "/v/ods/ods_20261108.jsonl")],
                               "k string, _source_file string")
    assert [r.k for r in latest_snapshot(df).collect()] == ["b"]
