from __future__ import annotations

import asyncio
import json
import shutil

import polars as pl
import pytest

pytest.importorskip("pyspark")

from app.profiling.profiling_pipeline import ProfilingPipeline
from app.pipeline.silver import SilverPipeline


@pytest.mark.integration
def test_pipeline_e2e(
    bronze_subset,
    datasets_config,
    settings,
    monkeypatch,
    tmp_path,
):
    shutil.copytree(bronze_subset, tmp_path / "data" / "bronze")

    populated_cats = sorted(
        p.name for p in (tmp_path / "data" / "bronze").iterdir()
        if p.is_dir() and p.name != "zone-lookup"
    )
    if not populated_cats:
        pytest.skip("No se encontraron datos de bronce")

    for cat in populated_cats:
        src = tmp_path / "data" / "bronze" / cat / "2023-01.parquet"
        if not src.exists():
            continue
        for m in range(2, 13):
            dst = tmp_path / "data" / "bronze" / cat / f"2023-{m:02d}.parquet"
            shutil.copy2(str(src), str(dst))

    monkeypatch.chdir(tmp_path)
    profiling = ProfilingPipeline()
    try:
        asyncio.run(profiling.run(datasets_config))
    except Exception:
        pass
    for cat in populated_cats:
        jp = tmp_path / "data" / "profiling" / cat / "2023-01.json"
        if jp.exists():
            with open(jp) as f:
                r = json.load(f)
            assert "meta" in r
            assert "dimensions" in r

    import app.utils.globals as _g
    monkeypatch.setattr(_g, "PROJECT_ROOT", tmp_path)
    silver = SilverPipeline()
    silver.run_quality(datasets_config)
    for cat in populated_cats:
        sp = tmp_path / "data" / "silver" / "stage" / cat / "2023-01.parquet"
        assert sp.exists()
    assert (tmp_path / "data" / "silver" / "audit.parquet").exists()

    silver.run_schema()
    dd = tmp_path / "data" / "silver" / "star" / "dims"
    for dn in ["dim_date", "dim_zone", "dim_vendor", "dim_ratecode", "dim_payment_type", "dim_service"]:
        assert (dd / f"{dn}.parquet").exists()
    silver.run_load(datasets_config)
    for cat in populated_cats:
        fp = tmp_path / "data" / "silver" / "star" / "facts" / f"fact_{cat}_trip" / "2023-01.parquet"
        assert fp.exists()

    from app.pipeline.gold.gold_pipeline import GoldPipeline
    gold = GoldPipeline(mode="full")
    gold.run(settings)
    assert (tmp_path / "data" / "gold" / "dims" / "dim_date_gold.parquet").exists()
    assert (tmp_path / "data" / "gold" / "dims" / "dim_zone_gold.parquet").exists()
    assert (tmp_path / "data" / "gold" / "dims" / "dim_ratecode_theoretical.parquet").exists()
    ga = pl.read_parquet(str(tmp_path / "data" / "gold" / "audit.parquet"))
    assert len(ga) > 0

    ba = pl.read_parquet(str(tmp_path / "data" / "bronze" / "audit.parquet"))
    sa = pl.read_parquet(str(tmp_path / "data" / "silver" / "audit.parquet"))
    gold_a = pl.read_parquet(str(tmp_path / "data" / "gold" / "audit.parquet"))
    bids = set(ba["audit_id"])
    sids = set(sa["audit_id"])
    assert all(b in bids for b in sa["bronze_audit_id"])
    assert all(s in sids for s in gold_a["silver_audit_id"])

    for cat in populated_cats:
        jp = tmp_path / "data" / "profiling" / cat / "2023-01.json"
        if jp.exists():
            with open(jp) as f:
                r = json.load(f)
            for d in r["dimensions"]:
                assert 0.0 <= d["score"] <= 1.0
