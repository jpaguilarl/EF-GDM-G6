from pathlib import Path

from app.utils import storage

PROJECT_ROOT: Path = storage.LOCAL_PROJECT_ROOT


class Globals:
    @property
    def project_root(self):
        if storage.get_backend() == "s3":
            return storage.get_root()
        # Referencia al nombre del modulo (no a storage.LOCAL_PROJECT_ROOT
        # directamente) para que los tests puedan seguir haciendo
        # monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_dir).
        return PROJECT_ROOT

    @property
    def tlc_categories(self):
        return ["green", "yellow", "fhv", "fhvhv"]


globals = Globals()
