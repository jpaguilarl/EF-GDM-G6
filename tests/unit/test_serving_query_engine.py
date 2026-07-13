from __future__ import annotations

import time
from pathlib import Path

import polars as pl
import pytest

from app.serving.query_engine import PolarsQueryEngine


@pytest.fixture
def marts_dir(tmp_path: Path) -> Path:
    base = tmp_path / "marts"
    mart_dir = base / "test_mart"
    rows = [
        {"service_id": "yellow", "year": 2025, "month": 1, "viajes": 100, "espera_promedio_min": 5.0, "categoria": "A"},
        {"service_id": "yellow", "year": 2025, "month": 2, "viajes": 200, "espera_promedio_min": 6.0, "categoria": "B"},
        {"service_id": "green", "year": 2025, "month": 1, "viajes": 150, "espera_promedio_min": 3.0, "categoria": "A"},
        {"service_id": "green", "year": 2025, "month": 2, "viajes": 250, "espera_promedio_min": 4.0, "categoria": "B"},
    ]
    partition_cols = {"service_id", "year", "month"}
    for row in rows:
        part_dir = mart_dir / f"service_id={row['service_id']}" / f"year={row['year']}" / f"month={row['month']}"
        part_dir.mkdir(parents=True, exist_ok=True)
        data_cols = {k: [v] for k, v in row.items() if k not in partition_cols}
        df = pl.DataFrame(data_cols)
        df.write_parquet(str(part_dir / "data.parquet"))
    return base


class TestPolarsQueryEngine:
    def test_scan(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        lf = engine.get_mart("test_mart")
        assert lf is not None
        df = lf.collect()
        assert len(df) == 4

    def test_filter_partition(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", filters={"service_id": ["yellow"]})
        assert len(df) == 2
        assert all(r["service_id"] == "yellow" for r in df.to_dicts())

    def test_filter_non_partition(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", filters={"categoria": ["A"]})
        assert len(df) == 2
        assert all(r["categoria"] == "A" for r in df.to_dicts())

    def test_filter_single_value(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", filters={"categoria": "A"})
        assert len(df) == 2
        assert all(r["categoria"] == "A" for r in df.to_dicts())

    def test_filter_multiple_columns(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query(
            "test_mart",
            filters={"service_id": ["yellow"], "month": [1]},
        )
        assert len(df) == 1
        row = df.to_dicts()[0]
        assert row["service_id"] == "yellow"
        assert row["month"] == 1

    def test_group_by_agg(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query(
            "test_mart",
            group_by=["service_id"],
            agg={"viajes": "sum", "espera_promedio_min": "mean"},
        )
        rows = {r["service_id"]: r for r in df.to_dicts()}
        assert rows["yellow"]["viajes"] == 300
        assert rows["green"]["viajes"] == 400
        assert rows["yellow"]["espera_promedio_min"] == 5.5
        assert rows["green"]["espera_promedio_min"] == 3.5

    def test_order_by(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", order_by=[("viajes", "desc")])
        vals = [r["viajes"] for r in df.to_dicts()]
        assert vals == sorted(vals, reverse=True)

    def test_order_by_multiple(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query(
            "test_mart",
            order_by=[("categoria", "asc"), ("viajes", "desc")],
        )
        rows = df.to_dicts()
        for i in range(len(rows) - 1):
            assert rows[i]["categoria"] <= rows[i + 1]["categoria"]
            if rows[i]["categoria"] == rows[i + 1]["categoria"]:
                assert rows[i]["viajes"] >= rows[i + 1]["viajes"]

    def test_limit(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", limit=2)
        assert len(df) == 2

    def test_nonexistent_mart(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("nonexistent")
        assert len(df) == 0

    def test_cache_hit(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir, cache_ttl=60)
        lf1 = engine.get_mart("test_mart")
        assert lf1 is not None
        lf2 = engine.get_mart("test_mart")
        assert lf2 is lf1

    def test_cache_expiry(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir, cache_ttl=1)
        lf1 = engine.get_mart("test_mart")
        assert lf1 is not None
        time.sleep(1.1)
        lf2 = engine.get_mart("test_mart")
        assert lf2 is not None
        assert lf2 is not lf1

    def test_no_filters_returns_all(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart")
        assert len(df) == 4

    def test_empty_filters_dict(self, marts_dir: Path):
        engine = PolarsQueryEngine(marts_dir)
        df = engine.query("test_mart", filters={})
        assert len(df) == 4
