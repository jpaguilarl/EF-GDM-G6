"""Constantes compartidas por los DAGs dag_01..dag_07.

Cada fase del pipeline (bronze, silver quality/schema/load, gold, profiling,
gold-ml) es su propio DAG encadenado con TriggerDagRunOperator — no un DAG
monolitico con varias tasks internas (ver AGENTS.md / plan de refactor). Todos
invocan el mismo CLI existente (`uv run main.py ...`) dentro del contenedor de
Airflow; ninguno reimplementa la logica de BronzePipeline/SilverPipeline/
GoldPipeline.
"""

from datetime import datetime, timedelta

PROJECT_DIR = "/opt/airflow/project"

DEFAULT_ARGS = {
    "owner": "tlc-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

START_DATE = datetime(2024, 1, 1)


def bash_command(cli_args: str) -> str:
    """Comando bash para invocar el CLI existente dentro del contenedor."""
    return f"cd {PROJECT_DIR} && uv run main.py {cli_args}".strip()
