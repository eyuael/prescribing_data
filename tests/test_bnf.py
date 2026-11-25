from ingestion.bnf import build_dim_bnf, landing_name, parse_resource_name, plan_downloads, to_silver_bnf

HEADERS = ["BNF_CHAPTER", "BNF_CHAPTER_CODE", "BNF_SECTION", "BNF_SECTION_CODE", "BNF_PARAGRAPH", "BNF_PARAGRAPH_CODE",
           "BNF_SUBPARAGRAPH", "BNF_SUBPARAGRAPH_CODE", "BNF_CHEMICAL_SUBSTANCE", "BNF_CHEMICAL_SUBSTANCE_CODE",
           "BNF_PRODUCT", "BNF_PRODUCT_CODE", "BNF_PRESENTATION", "BNF_PRESENTATION_CODE", "_source_file"]


def row(code, name, chem="Chem", chem_code="0302000C0", file="/v/bnf_historic_2025_v88.csv", section="Asthma"):
    return ("Respiratory", "03", section, "0302", "Corticosteroids", "030200", "Sub", "0302000", chem, chem_code,
            "Prod", code[:11], name, code, file)


def snaps(spark, rows):
    return to_silver_bnf(spark.createDataFrame(rows, HEADERS))


def test_parse_resource_names():
    assert parse_resource_name("BNF_CODE_HISTORIC_2025_VERSION_88") == {
        "kind": "historic", "period": "2025", "version": 88, "snapshot_key": "2025-12"}
    assert parse_resource_name("BNF_CODE_CURRENT_202610_VERSION_90")["snapshot_key"] == "2026-10"
    assert parse_resource_name("BNF_CODE_MONTHLY_CHANGES_202610") is None
    assert landing_name(parse_resource_name("BNF_CODE_CURRENT_202610_VERSION_90")) == "bnf_current_202610_v90.csv"


def test_plan_downloads_takes_all_historic_but_only_newest_current():
    res = [{"name": f"BNF_CODE_HISTORIC_{y}_VERSION_{v}", "url": f"u{y}"} for y, v in [(2024, 86), (2025, 88)]]
    res += [{"name": f"BNF_CODE_CURRENT_2026{m:02d}_VERSION_90", "url": f"c{m}"} for m in (8, 10, 9)]
    got = [(i["kind"], i["period"]) for i, _ in plan_downloads(res)]
    assert got == [("historic", "2024"), ("historic", "2025"), ("current", "202610")]


def test_to_silver_reads_snapshot_from_file_name_and_cleans(spark):
    r = snaps(spark, [row(" 0302000c0bqaabx ", "Fostair inhaler", file="/v/bnf_current_202610_v90.csv")]).first()
    assert (r.snapshot_key, r.snapshot_kind, r.version) == ("2026-10", "current", 90)
    assert r.presentation_code == "0302000C0BQAABX" and r.chapter_code == "03"


def test_dim_bnf_follows_a_recode_to_todays_hierarchy(spark):
    old, new = "0302000C0BQAABX", "0302000ABBCAAAB"
    h25, c26 = "/v/bnf_historic_2025_v88.csv", "/v/bnf_current_202610_v90.csv"
    d = {r.presentation_code: r for r in build_dim_bnf(snaps(spark, [
        row(old, "Fostair 100 inhaler", chem="Beclometasone/formoterol", chem_code="0302000C0", file=h25),
        row("0101010A0AAAAAA", "Alexitol 360mg tablets", chem="Alexitol", chem_code="0101010A0", file=h25),
        row("0101010A0AAAAAA", "Alexitol 360mg tablets", chem="Alexitol", chem_code="0101010A0", file=c26),
        row(new, "Fostair 100 inhaler", chem="Beclometasone/formoterol", chem_code="0302000AB", file=c26),
    ])).collect()}
    assert d[old].lineage_method == "same_name" and d[old].current_presentation_code == new
    assert d[old].chemical_substance_code == "0302000AB"        # history expressed in today's group
    assert d[old].is_current is False and d[old].first_seen_snapshot == "2025-12"
    assert d[new].lineage_method == "current" and d[new].current_presentation_code == new
    assert d["0101010A0AAAAAA"].first_seen_snapshot == "2025-12" and d["0101010A0AAAAAA"].is_current


def test_ambiguous_or_missing_successor_keeps_last_known_hierarchy(spark):
    h25, c26 = "/v/bnf_historic_2025_v88.csv", "/v/bnf_current_202610_v90.csv"
    d = {r.presentation_code: r for r in build_dim_bnf(snaps(spark, [
        row("AAAAAAAAAAAAAAA", "Twin", chem_code="CHEM_OLD", file=h25),
        row("BBBBBBBBBBBBBBB", "Gone for good", chem_code="CHEM_GONE", file=h25),
        row("CCCCCCCCCCCCCCC", "Twin", chem_code="CHEM_NEW1", file=c26),
        row("DDDDDDDDDDDDDDD", "twin!", chem_code="CHEM_NEW2", file=c26),   # same normalised name: ambiguous
    ])).collect()}
    assert d["AAAAAAAAAAAAAAA"].lineage_method == "retired_no_successor"
    assert d["AAAAAAAAAAAAAAA"].current_presentation_code is None
    assert d["AAAAAAAAAAAAAAA"].chemical_substance_code == "CHEM_OLD"     # still resolvable
    assert d["BBBBBBBBBBBBBBB"].lineage_method == "retired_no_successor"


def test_dim_bnf_has_one_row_per_code(spark):
    rows = [row("0101010A0AAAAAA", "Alexitol", file=f) for f in
            ("/v/bnf_historic_2024_v86.csv", "/v/bnf_historic_2025_v88.csv", "/v/bnf_current_202610_v90.csv")]
    assert build_dim_bnf(snaps(spark, rows)).count() == 1
