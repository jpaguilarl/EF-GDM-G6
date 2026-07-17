import polars as pl

from app.panel.audit_reader import read_audit_lineage


def _make_bronze(tmp_path):
    path = tmp_path / "data/bronze/audit.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame([
        {
            "audit_id": "b-1111", "name": "yellow_2025-01", "source_file": "yellow/2025-01.parquet",
            "bytecount": 500, "rowcount": 1000,
            "start_timestamp": "2025-01-01T00:00:00", "end_timestamp": "2025-01-01T01:00:00",
        },
        {
            "audit_id": "b-2222", "name": "green_2025-01", "source_file": "green/2025-01.parquet",
            "bytecount": 300, "rowcount": 500,
            "start_timestamp": "2025-01-02T00:00:00", "end_timestamp": "2025-01-02T00:30:00",
        },
    ])
    df.write_parquet(str(path))


def _make_silver(tmp_path):
    path = tmp_path / "data/silver/audit.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame([
        {
            "audit_id": "s-1111", "bronze_audit_id": "b-1111",
            "source_file": "yellow/2025-01.parquet",
            "rowcount_bronze": 1000, "rowcount_quality": 950, "rowcount_quarantined": 50,
            "start_timestamp": "2025-01-01T02:00:00", "end_timestamp": "2025-01-01T02:15:00",
        },
        {
            "audit_id": "s-2222", "bronze_audit_id": "b-2222",
            "source_file": "green/2025-01.parquet",
            "rowcount_bronze": 500, "rowcount_quality": 480, "rowcount_quarantined": 20,
            "start_timestamp": "2025-01-02T01:00:00", "end_timestamp": "2025-01-02T01:10:00",
        },
    ])
    df.write_parquet(str(path))


def _make_gold(tmp_path):
    path = tmp_path / "data/gold/audit.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame([
        {
            "gold_audit_id": "g-1111", "silver_audit_id": "s-1111",
            "mart_name": "mart_demand_volume", "subdir": "marts",
            "mode": "full", "start_timestamp": "2025-01-01T03:00:00",
            "end_timestamp": "2025-01-01T03:30:00", "rowcount_output": 9000,
            "partition_keys": "fecha_viaje", "config_snapshot_md5": "abc123",
        },
    ])
    df.write_parquet(str(path))


def test_lineage_all_layers(monkeypatch, tmp_path):
    _make_bronze(tmp_path)
    _make_silver(tmp_path)
    _make_gold(tmp_path)
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)

    result = read_audit_lineage(limit=50, offset=0)

    assert result["total"] == 5
    assert len(result["rows"]) == 5

    layers = [r["layer"] for r in result["rows"]]
    # sorted by start_timestamp desc: Jan 2 > Jan 1
    assert layers == ["silver", "bronze", "gold", "silver", "bronze"]

    gold = result["rows"][2]
    assert gold["layer"] == "gold"
    assert gold["audit_id"] == "g-1111"
    assert gold["fk_audit_id"] == "s-1111"
    assert gold["source_name"] == "mart_demand_volume"
    assert gold["rows_in"] is None
    assert gold["rows_out"] == 9000
    assert gold["rows_rejected"] is None
    assert gold["duration_sec"] == 1800.0

    silver = result["rows"][0]
    assert silver["layer"] == "silver"
    assert silver["audit_id"] == "s-2222"
    assert silver["fk_audit_id"] == "b-2222"
    assert silver["source_name"] == "green/2025-01.parquet"
    assert silver["rows_in"] == 500
    assert silver["rows_out"] == 480
    assert silver["rows_rejected"] == 20
    assert silver["duration_sec"] == 600.0  # 10 min

    bronze = result["rows"][1]
    assert bronze["layer"] == "bronze"
    assert bronze["audit_id"] == "b-2222"
    assert bronze["fk_audit_id"] is None
    assert bronze["source_name"] == "green/2025-01.parquet"
    assert bronze["rows_in"] is None
    assert bronze["rows_out"] == 500
    assert bronze["rows_rejected"] is None
    assert bronze["duration_sec"] == 1800.0  # 30 min


def test_lineage_layer_filter(monkeypatch, tmp_path):
    _make_bronze(tmp_path)
    _make_silver(tmp_path)
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)

    result = read_audit_lineage(layer="bronze", limit=50, offset=0)

    assert result["total"] == 2
    assert all(r["layer"] == "bronze" for r in result["rows"])


def test_lineage_missing_files(monkeypatch, tmp_path):
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)

    result = read_audit_lineage(limit=50, offset=0)

    assert result["total"] == 0
    assert result["rows"] == []


def test_lineage_pagination(monkeypatch, tmp_path):
    _make_bronze(tmp_path)
    _make_silver(tmp_path)
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)

    result = read_audit_lineage(limit=2, offset=0)
    assert result["total"] == 4
    assert len(result["rows"]) == 2

    result2 = read_audit_lineage(limit=2, offset=2)
    assert result2["total"] == 4
    assert len(result2["rows"]) == 2
    # ensure no overlap
    ids_1 = {r["audit_id"] for r in result["rows"]}
    ids_2 = {r["audit_id"] for r in result2["rows"]}
    assert ids_1 & ids_2 == set()
