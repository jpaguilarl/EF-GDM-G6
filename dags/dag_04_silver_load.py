"""Fase 4/6: silver carga (tablas de hechos del modelo estrella).

Requiere que dag_03_silver_schema haya completado (dimensiones existentes).
Al terminar, dispara gold (incremental).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_04_silver_load",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "silver"],
) as dag:
    silver_load = BashOperator(
        task_id="silver_load",
        bash_command=bash_command("--silver load"),
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="dag_05_gold",
        wait_for_completion=True,
        poke_interval=30,
    )

    silver_load >> trigger_gold
