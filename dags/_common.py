"""Constantes compartidas por los DAGs dag_01..dag_07.

Cada fase del pipeline (bronze, silver quality/schema/load, gold, profiling,
gold-ml) es su propio DAG encadenado con TriggerDagRunOperator — no un DAG
monolitico con varias tasks internas (ver AGENTS.md / plan de refactor). Todos
invocan el mismo CLI existente (`uv run main.py ...`) dentro del contenedor de
Airflow; ninguno reimplementa la logica de BronzePipeline/SilverPipeline/
GoldPipeline.
"""

from datetime import datetime, timedelta

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_DIR = "/opt/airflow/project"

DEFAULT_ARGS = {
    "owner": "tlc-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

START_DATE = datetime(2024, 1, 1)

# Kwargs de DAG compartidos para toda la cadena batch (dag_01..dag_06). Un solo
# DagRun activo por DAG: un re-disparo o un backfill no puede solapar dos
# corridas del mismo DAG escribiendo a la misma ruta (la carrera que rompio los
# facts en S3). catchup=False evita que Airflow encole corridas historicas.
DAG_KWARGS = {
    "default_args": DEFAULT_ARGS,
    "schedule": None,
    "start_date": START_DATE,
    "catchup": False,
    "max_active_runs": 1,
}


def bash_command(cli_args: str) -> str:
    """Comando bash para invocar el CLI existente dentro del contenedor."""
    return f"cd {PROJECT_DIR} && uv run main.py {cli_args}".strip()


def trigger_next(task_id: str, trigger_dag_id: str) -> TriggerDagRunOperator:
    """TriggerDagRunOperator endurecido contra la duplicacion de corridas.

    El default de la cadena reintentaba la tarea de trigger (hereda retries=1 de
    DEFAULT_ARGS); como el run_id por defecto lleva timestamp, cada reintento
    disparaba una SEGUNDA corrida del DAG hijo -> dos Spark escribiendo al mismo
    prefijo S3 (carrera de commit) y cascada de subprocesos aguas abajo.

    Endurecimiento:
    - trigger_run_id="{{ run_id }}": el run_id del hijo = run_id del padre,
      CONSTANTE entre reintentos del trigger. Un re-disparo apunta a la misma
      corrida, no crea una nueva.
    - skip_when_already_exists=True (Airflow 2.10+): si esa corrida hija ya
      existe, se salta en vez de duplicar / lanzar DagRunAlreadyExists.
    - retries=0: una espera larga (Spark de 10-15 min) NO debe re-disparar la
      cadena. Sobrescribe el retries=1 de DEFAULT_ARGS solo para el trigger.
    - poke_interval=60: menos LIST/poking sobre un hijo que tarda minutos.
    """
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=True,
        poke_interval=60,
        trigger_run_id="{{ run_id }}",
        skip_when_already_exists=True,
        retries=0,
        failed_states=["failed"],
    )
