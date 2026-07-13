from pathlib import Path

import yaml

from app.schemas.settings_schema import SettingsSchema

PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings:
    def __init__(
        self,
        settings_path: str | Path = PROJECT_ROOT / "config.yaml",
    ):
        with open(settings_path) as f:
            loaded_config = yaml.safe_load(f)
            self._config = SettingsSchema.model_validate(loaded_config)

    @property
    def config(self) -> SettingsSchema:
        return self._config


settings = Settings().config
