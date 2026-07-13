from pathlib import Path

import polars as pl
import pytest

from app.speed.zone_lookup import ZoneLookup


@pytest.fixture
def zone_parquet(tmp_path: Path) -> Path:
    path = tmp_path / "dim_zone.parquet"
    df = pl.DataFrame({
        "LocationID": [1, 2, 3],
        "Borough": ["Manhattan", "Brooklyn", "Queens"],
        "Zone": ["Midtown", "Williamsburg", "Astoria"],
        "borough_name_es": ["Manhattan", "Brooklyn", "Queens"],
    })
    df.write_parquet(str(path))
    return path


class TestZoneLookup:
    def test_load_and_lookup(self, zone_parquet: Path):
        lookup = ZoneLookup()
        lookup.load(zone_parquet)
        result = lookup.lookup(1)
        assert result is not None
        assert result["borough"] == "Manhattan"
        assert result["zone"] == "Midtown"
        assert result["borough_es"] == "Manhattan"

    def test_lookup_unknown_id(self, zone_parquet: Path):
        lookup = ZoneLookup()
        lookup.load(zone_parquet)
        result = lookup.lookup(999)
        assert result is None

    def test_lookup_none(self, zone_parquet: Path):
        lookup = ZoneLookup()
        lookup.load(zone_parquet)
        result = lookup.lookup(None)
        assert result is None

    def test_lookup_multiple_ids(self, zone_parquet: Path):
        lookup = ZoneLookup()
        lookup.load(zone_parquet)
        r1 = lookup.lookup(2)
        r2 = lookup.lookup(3)
        assert r1["borough"] == "Brooklyn"
        assert r2["zone"] == "Astoria"

    def test_load_without_borough_es(self, tmp_path: Path):
        path = tmp_path / "dim_zone_no_es.parquet"
        df = pl.DataFrame({
            "LocationID": [10],
            "Borough": ["Bronx"],
            "Zone": ["Pelham"],
        })
        df.write_parquet(str(path))
        lookup = ZoneLookup()
        lookup.load(path)
        result = lookup.lookup(10)
        assert result["borough_es"] == "Bronx"
