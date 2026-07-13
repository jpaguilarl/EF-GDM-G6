import json

import joblib
import numpy as np
import pytest
from sklearn.ensemble import IsolationForest

from app.speed.ml_state import ModelLoader


def _make_if_model(path, random_state=42):
    path.mkdir(parents=True, exist_ok=True)
    X = np.random.default_rng(random_state).normal(size=(100, 6))
    model = IsolationForest(
        n_estimators=10,
        contamination=0.1,
        random_state=random_state,
        n_jobs=1,
    )
    model.fit(X)
    joblib.dump(model, str(path / "model.joblib"))


def _make_kmodes_model(path, service_id, random_state=42):
    from kmodes.kmodes import KModes

    path.mkdir(parents=True, exist_ok=True)
    X = np.array([[0, 0, 0], [1, 1, 1], [2, 2, 2], [0, 1, 0]], dtype=np.int32)
    model = KModes(n_clusters=2, init="Cao", n_init=1, random_state=random_state)
    model.fit(X)
    joblib.dump(model, str(path / "model.joblib"))

    mapping = {
        "borough_pu": {"0": "Manhattan", "1": "Brooklyn"},
        "borough_do": {"0": "Queens", "1": "Bronx"},
    }
    with open(str(path / "category_mapping.json"), "w") as f:
        json.dump(mapping, f)


def test_load_if_models(tmp_path, monkeypatch):
    models_root = tmp_path / "data/gold/models/isolation_forest"
    _make_if_model(models_root / "1")
    _make_if_model(models_root / "2", random_state=123)

    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)
    loader = ModelLoader()
    loader.load()

    assert 1 in loader.if_models
    assert 2 in loader.if_models
    assert isinstance(loader.if_models[1], IsolationForest)


def test_load_kmodes_models(tmp_path, monkeypatch):
    models_root = tmp_path / "data/gold/models/kmodes"
    _make_kmodes_model(models_root / "yellow", "yellow")

    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)
    loader = ModelLoader()
    loader.load()

    assert "yellow" in loader.kmodes_models
    assert "yellow" in loader.kmodes_mappings
    assert "borough_pu" in loader.kmodes_mappings["yellow"]


def test_load_empty_models_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)
    loader = ModelLoader()
    loader.load()
    assert len(loader.if_models) == 0
    assert len(loader.kmodes_models) == 0


def test_load_handles_corrupted_joblib(tmp_path, monkeypatch):
    models_root = tmp_path / "data/gold/models/isolation_forest/1"
    models_root.mkdir(parents=True, exist_ok=True)
    with open(str(models_root / "model.joblib"), "w") as f:
        f.write("not a valid joblib file")

    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path)
    loader = ModelLoader()
    loader.load()
    assert len(loader.if_models) == 0


def test_flat_fares_loaded():
    loader = ModelLoader()
    assert 2 in loader.flat_fares
    assert 2025 in loader.flat_fares[2]
    assert loader.flat_fares[2][2025] == 70.0
