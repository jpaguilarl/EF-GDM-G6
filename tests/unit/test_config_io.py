import pydantic
from pathlib import Path

import pytest
import yaml

from app.panel.config_io import _deep_merge, config_path, read_config, write_config


FIXTURE_CONFIG = {
    "storage": {"backend": "local"},
    "datasets": {"years": [2023, 2024, 2025]},
    "gold": {
        "mode": "full",
        "supply_demand": {"block_minutes": 15, "deficit_threshold": -10},
        "abc_xyz": {"class_a_pct": 0.8, "class_b_pct": 0.15, "xyz_x_max": 0.2, "xyz_y_max": 0.5},
        "isolation_fraud": {"contamination": 0.05, "n_estimators": 100},
        "kmodes": {"max_k": 8, "n_init": 2, "init_method": "Cao", "random_state": 42},
    },
    "serving": {"host": "0.0.0.0", "port": 8000},
}


def _write_fixture(path: Path):
    with open(path, "w") as f:
        yaml.safe_dump(FIXTURE_CONFIG, f, sort_keys=False, allow_unicode=True, default_flow_style=False)


def test_deep_merge_nested_field(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    _write_fixture(cfg_path)
    monkeypatch.setattr("app.panel.config_io.config_path", lambda: cfg_path)

    write_config({"gold": {"supply_demand": {"block_minutes": 7}}})

    result = read_config()
    assert result["gold"]["mode"] == "full"
    assert result["gold"]["supply_demand"]["block_minutes"] == 7
    assert result["gold"]["supply_demand"]["deficit_threshold"] == -10
    assert result["gold"]["abc_xyz"]["class_a_pct"] == 0.8
    assert result["gold"]["kmodes"]["init_method"] == "Cao"
    assert result["storage"]["backend"] == "local"
    assert result["datasets"]["years"] == [2023, 2024, 2025]
    assert result["serving"]["host"] == "0.0.0.0"


def test_deep_merge_replaces_list(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    _write_fixture(cfg_path)
    monkeypatch.setattr("app.panel.config_io.config_path", lambda: cfg_path)

    write_config({"datasets": {"years": [2026]}})

    result = read_config()
    assert result["datasets"]["years"] == [2026]
    assert result["storage"]["backend"] == "local"


def test_deep_merge_multiple_top_level(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    _write_fixture(cfg_path)
    monkeypatch.setattr("app.panel.config_io.config_path", lambda: cfg_path)

    write_config({"storage": {"backend": "s3"}, "serving": {"port": 9000}})

    result = read_config()
    assert result["storage"]["backend"] == "s3"
    assert result["serving"]["port"] == 9000
    assert result["gold"]["mode"] == "full"


def test_deep_merge_exact_equivalent():
    base = {"a": {"b": 1, "c": 2}, "d": [1, 2]}
    partial = {"a": {"b": 99, "e": 3}, "f": 4}
    merged = _deep_merge(base, partial)
    assert merged["a"]["b"] == 99
    assert merged["a"]["c"] == 2
    assert merged["a"]["e"] == 3
    assert merged["d"] == [1, 2]
    assert merged["f"] == 4


def test_write_config_validates(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    _write_fixture(cfg_path)
    monkeypatch.setattr("app.panel.config_io.config_path", lambda: cfg_path)

    import pydantic

    with pytest.raises(pydantic.ValidationError):
        write_config({"storage": {"backend": "invalid_value"}})


def test_write_config_atomic(tmp_path: Path, monkeypatch):
    cfg_path = tmp_path / "config.yaml"
    _write_fixture(cfg_path)
    monkeypatch.setattr("app.panel.config_io.config_path", lambda: cfg_path)

    try:
        write_config({"gold": {"supply_demand": {"block_minutes": "nope"}}})
    except (TypeError, pydantic.ValidationError):
        pass

    result = read_config()
    assert result["gold"]["supply_demand"]["block_minutes"] == 15
