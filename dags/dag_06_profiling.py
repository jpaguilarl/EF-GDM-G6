"""Fase 6/6: profiling (calidad de bronce, 8 dimensiones) -> data/profiling/.

Ultima fase del batch principal. No dispara dag_07_gold_ml automaticamente: el
entrenamiento de modelos es pesado y no siempre se quiere en cada corrida (ver
plan de refactor) — se dispara manualmente desde la UI de Airflow cuando se
quiera reentrenar.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command

with DAG(
    dag_id="dag_06_profiling",
    tags=["tlc", "profiling"],
    **DAG_KWARGS,
) as dag:
    profiling = BashOperator(
        task_id="profiling",
        bash_command=bash_command("--profile"),
    )
