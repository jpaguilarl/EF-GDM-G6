from __future__ import annotations

import shutil

import polars as pl
import pytest

pytest.importorskip("pyspark")

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

    import app.utils.globals as _g
    monkeypatch.setattr(_g, "PROJECT_ROOT", tmp_path)
    silver = SilverPipeline()
    silver.run_quality(datasets_config)
    for cat in populated_cats:
        sp = tmp_path / "data" / "silver" / "stage" / cat / "2025-01.parquet"
        assert sp.exists()
    assert (tmp_path / "data" / "silver" / "audit.parquet").exists()

    silver.run_schema()
    dd = tmp_path / "data" / "silver" / "star" / "dims"
    for dn in ["dim_date", "dim_zone", "dim_ratecode", "dim_payment_type"]:
        assert (dd / f"{dn}.parquet").exists()
    silver.run_load(datasets_config)
    for cat in populated_cats:
        fp = tmp_path / "data" / "silver" / "star" / "facts" / f"fact_{cat}_trip" / "2025-01.parquet"
        st = tmp_path / "data" / "silver" / "stage" / cat / "2025-01.parquet"
        # Si el stage tiene filas debe tener fact; si tiene 0 filas (todo rechazado) se omite
        if (st / "_SUCCESS").exists() and pl.scan_parquet(str(st / "part-*.parquet")).collect().height > 0:
            assert fp.exists()

    from app.pipeline.gold import GoldPipeline
    gold = GoldPipeline(mode="full", only=["mart_demand_volume"])
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
