import json
from pathlib import Path

import joblib
from kmodes.kmodes import KModes
from sklearn.ensemble import IsolationForest

from app.pipeline.gold_impl.feature_rules import ratecode_tariff as rt
from app.utils.globals import globals


class ModelLoader:
    def __init__(self):
        self._if_models: dict[int, IsolationForest] = {}
        self._kmodes_models: dict[str, KModes] = {}
        self._kmodes_mappings: dict[str, dict] = {}
        self._flat_fares: dict[int, dict[int, float]] = rt.FLAT_FARES

    def load(self) -> None:
        self._load_if_models()
        self._load_kmodes_models()

    def _load_if_models(self) -> None:
        models_dir = globals.project_root / "data/gold/models/isolation_forest"
        if not models_dir.exists():
            return
        for rc_dir in models_dir.iterdir():
            if not rc_dir.is_dir():
                continue
            model_path = rc_dir / "model.joblib"
            if model_path.exists():
                try:
                    model = joblib.load(str(model_path))
                    self._if_models[int(rc_dir.name)] = model
                except Exception:
                    pass

    def _load_kmodes_models(self) -> None:
        models_dir = globals.project_root / "data/gold/models/kmodes"
        if not models_dir.exists():
            return
        for svc_dir in models_dir.iterdir():
            if not svc_dir.is_dir():
                continue
            model_path = svc_dir / "model.joblib"
            mapping_path = svc_dir / "category_mapping.json"
            if model_path.exists():
                try:
                    model = joblib.load(str(model_path))
                    self._kmodes_models[svc_dir.name] = model
                except Exception:
                    pass
            if mapping_path.exists():
                with open(str(mapping_path)) as f:
                    self._kmodes_mappings[svc_dir.name] = json.load(f)

    @property
    def if_models(self) -> dict[int, IsolationForest]:
        return self._if_models

    @property
    def kmodes_models(self) -> dict[str, KModes]:
        return self._kmodes_models

    @property
    def kmodes_mappings(self) -> dict[str, dict]:
        return self._kmodes_mappings

    @property
    def flat_fares(self) -> dict[int, dict[int, float]]:
        return self._flat_fares
