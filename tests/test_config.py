from ingestion.config import Config


def test_months_spans_year_boundary():
    assert Config("202411", "202502").months() == ["202411", "202412", "202501", "202502"]


def test_months_single_month():
    assert Config("202501", "202501").months() == ["202501"]


def test_source_switches_at_snomed_from():
    cfg = Config("202506", "202507")
    assert cfg.source_for("202506") == "legacy"
    assert cfg.source_for("202507") == "snomed"


def test_table_names():
    cfg = Config("202501", "202501", catalog="c", schema="s")
    assert cfg.silver == "c.s.silver_epd"
    assert cfg.bronze("legacy") == "c.s.bronze_epd_legacy"
    assert cfg.vol == "/Volumes/c/s/raw"
