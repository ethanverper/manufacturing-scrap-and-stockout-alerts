"""The central configuration must load and be consistent with itself."""

from __future__ import annotations

from src.common.settings import master_data, settings


def test_alert_thresholds_match_the_spec() -> None:
    """docs/spec.md: reject rate above its 3-sigma control limit and the 3% floor, coverage < 1 day."""
    alerts = settings()["alerts"]
    assert alerts["reject_rate_floor_pct"] == 3.0
    assert alerts["reject_rate_sigma"] == 3.0
    assert alerts["stock_coverage_threshold_days"] == 1.0


def test_every_bom_part_exists_in_master_data() -> None:
    """A BOM that names an unknown part would break planned usage and scrap cost."""
    data = master_data()
    bom_parts = {part for bom in data["station_bom"].values() for part in bom}
    assert bom_parts == set(data["parts"])


def test_every_part_supplier_exists() -> None:
    """Each part must point to a known supplier (the supplier level of the KPI hierarchy)."""
    data = master_data()
    assert {spec["supplier"] for spec in data["parts"].values()} <= set(data["suppliers"])


def test_incidents_point_to_real_plants_and_parts() -> None:
    """The story must reference plants, lines and parts that exist, or it silently does nothing."""
    config = settings()
    incidents = config["simulation"]["incidents"]
    parts = set(master_data()["parts"])
    assert incidents["bad_lot"]["plant"] in config["plant"]["plants"]
    assert incidents["bad_lot"]["part"] in parts
    assert incidents["shortage"]["part"] in parts
    assert incidents["line_behind"]["line"] in config["plant"]["lines"]
