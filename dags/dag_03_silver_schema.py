"""Fase 3/6: silver esquema (dimensiones del modelo estrella).

Debe completar ANTES de silver carga: las facts (dag_04) hacen join contra
estas dimensiones. Dependencia dura, documentada en AGENTS.md.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_03_silver_schema",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "silver"],
) as dag:
    silver_schema = BashOperator(
        task_id="silver_schema",
        bash_command=bash_command("--silver schema"),
    )

    trigger_silver_load = TriggerDagRunOperator(
        task_id="trigger_silver_load",
        trigger_dag_id="dag_04_silver_load",
        wait_for_completion=True,
        poke_interval=30,
    )

    silver_schema >> trigger_silver_load
