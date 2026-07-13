"""Fase 2/6: silver calidad (bronce -> stage/reject, ver SilverCleaner).

Idempotente por mes (skip si stage/reject ya tienen `_SUCCESS`). Al terminar,
dispara silver esquema (las dimensiones deben existir antes de silver carga).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command, trigger_next

with DAG(
    dag_id="dag_02_silver_quality",
    tags=["tlc", "silver"],
    **DAG_KWARGS,
) as dag:
    silver_quality = BashOperator(
        task_id="silver_quality",
        bash_command=bash_command("--silver quality"),
    )

    trigger_silver_schema = trigger_next(
        "trigger_silver_schema", "dag_03_silver_schema"
    )

    silver_quality >> trigger_silver_schema
