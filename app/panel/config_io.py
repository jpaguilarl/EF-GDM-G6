from pathlib import Path

import yaml

from app.utils.globals import globals

NON_SECRET_ENV_KEYS = {
    "STORAGE_BACKEND", "S3_BUCKET", "S3_PREFIX", "REDIS_URL",
    "AIRFLOW_UID", "SPARK_DRIVER_MEMORY", "SPARK_MASTER_CORES", "SPARK_LOCAL_DIR",
}
SECRET_ENV_KEYS = {
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    "AIRFLOW__CORE__FERNET_KEY", "_AIRFLOW_WWW_USER_PASSWORD",
}


def config_path() -> Path:
    return globals.project_root / "config.yaml"


def read_config() -> dict:
    with open(config_path()) as f:
        return yaml.safe_load(f)


def _deep_merge(base: dict, partial: dict) -> dict:
    for k, v in partial.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def write_config(partial: dict) -> None:
    from app.schemas.settings_schema import SettingsSchema

    current = read_config()
    _deep_merge(current, partial)

    SettingsSchema.model_validate(current)

    tmp = config_path().with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.safe_dump(current, f, sort_keys=False, allow_unicode=True, default_flow_style=False)
    tmp.rename(config_path())


def dotenv_path() -> Path:
    return globals.project_root / ".env"


def read_env() -> dict:
    path = dotenv_path()
    result = {}
    if not path.exists():
        for k in NON_SECRET_ENV_KEYS:
            result[k] = ""
        for k in SECRET_ENV_KEYS:
            result[k] = "<unset>"
        return result

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("\"'")
            if key in NON_SECRET_ENV_KEYS:
                result[key] = val
            elif key in SECRET_ENV_KEYS:
                result[key] = "<set>" if val else "<unset>"

    for k in NON_SECRET_ENV_KEYS:
        result.setdefault(k, "")
    for k in SECRET_ENV_KEYS:
        result.setdefault(k, "<unset>")

    return result


def write_env(updates: dict) -> None:
    path = dotenv_path()

    lines = []
    if path.exists():
        with open(path) as f:
            lines = f.readlines()

    for key, value in updates.items():
        if key not in NON_SECRET_ENV_KEYS:
            continue
        found = False
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(f"{key}="):
                lines[i] = f"{key}={value}\n"
                found = True
                break
        if not found:
            lines.append(f"{key}={value}\n")

    with open(path, "w") as f:
        f.writelines(lines)
