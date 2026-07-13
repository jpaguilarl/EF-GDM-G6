"""Fase 5/6: gold incremental (marts Power BI + feature stores ML).

Aborta limpio (via el propio CLI) si falta data/silver/star/. Al terminar,
dispara profiling — a proposito AL FINAL, como en run_full_pipeline: es
documentacion de solo lectura que no alimenta a silver/gold, asi los marts
quedan listos antes.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command, trigger_next

with DAG(
    dag_id="dag_05_gold",
    tags=["tlc", "gold"],
    **DAG_KWARGS,
) as dag:
    gold_incremental = BashOperator(
        task_id="gold_incremental",
        bash_command=bash_command("--gold incremental"),
    )

    trigger_profiling = trigger_next("trigger_profiling", "dag_06_profiling")

    gold_incremental >> trigger_profiling
