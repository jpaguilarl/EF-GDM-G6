"""Fase 3/6: silver esquema (dimensiones del modelo estrella).

Debe completar ANTES de silver carga: las facts (dag_04) hacen join contra
estas dimensiones. Dependencia dura, documentada en AGENTS.md.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command, trigger_next

with DAG(
    dag_id="dag_03_silver_schema",
    tags=["tlc", "silver"],
    **DAG_KWARGS,
) as dag:
    silver_schema = BashOperator(
        task_id="silver_schema",
        bash_command=bash_command("--silver schema"),
    )

    trigger_silver_load = trigger_next(
        "trigger_silver_load", "dag_04_silver_load"
    )

    silver_schema >> trigger_silver_load
