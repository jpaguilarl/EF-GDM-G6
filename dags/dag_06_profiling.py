"""Fase 6/6: profiling (calidad de bronce, 8 dimensiones) -> data/profiling/.

Ultima fase del batch principal. No dispara dag_07_gold_ml automaticamente: el
entrenamiento de modelos es pesado y no siempre se quiere en cada corrida (ver
plan de refactor) — se dispara manualmente desde la UI de Airflow cuando se
quiera reentrenar.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_06_profiling",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "profiling"],
) as dag:
    profiling = BashOperator(
        task_id="profiling",
        bash_command=bash_command("--profile"),
    )
