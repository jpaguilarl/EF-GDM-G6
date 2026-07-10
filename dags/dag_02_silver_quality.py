"""Fase 2/6: silver calidad (bronce -> stage/reject, ver SilverCleaner).

Idempotente por mes (skip si stage/reject ya tienen `_SUCCESS`). Al terminar,
dispara silver esquema (las dimensiones deben existir antes de silver carga).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_02_silver_quality",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "silver"],
) as dag:
    silver_quality = BashOperator(
        task_id="silver_quality",
        bash_command=bash_command("--silver quality"),
    )

    trigger_silver_schema = TriggerDagRunOperator(
        task_id="trigger_silver_schema",
        trigger_dag_id="dag_03_silver_schema",
        wait_for_completion=True,
        poke_interval=30,
    )

    silver_quality >> trigger_silver_schema
