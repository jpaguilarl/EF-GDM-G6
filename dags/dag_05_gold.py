"""Fase 5/6: gold incremental (marts Power BI + feature stores ML).

Aborta limpio (via el propio CLI) si falta data/silver/star/. Al terminar,
dispara profiling — a proposito AL FINAL, como en run_full_pipeline: es
documentacion de solo lectura que no alimenta a silver/gold, asi los marts
quedan listos antes.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_05_gold",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "gold"],
) as dag:
    gold_incremental = BashOperator(
        task_id="gold_incremental",
        bash_command=bash_command("--gold incremental"),
    )

    trigger_profiling = TriggerDagRunOperator(
        task_id="trigger_profiling",
        trigger_dag_id="dag_06_profiling",
        wait_for_completion=True,
        poke_interval=30,
    )

    gold_incremental >> trigger_profiling
