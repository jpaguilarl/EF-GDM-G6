from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


class Globals:
    @property
    def project_root(self):
        return PROJECT_ROOT

    @property
    def tlc_categories(self):
        return ["green", "yellow", "fhv", "fhvhv"]


globals = Globals()
