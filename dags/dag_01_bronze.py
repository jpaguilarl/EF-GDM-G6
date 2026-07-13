"""Fase 1/6: descarga bronce (zone-lookup + parquet mensuales por categoria).

Idempotente (skip si el footer parquet ya es legible). La segunda descarga es
un reintento barato: CloudFront responde 403 transitorios en rafagas grandes,
pero un archivo ya valido se omite instantaneamente (mismo patron que
run_full_pipeline en main.py). Al terminar, dispara silver quality.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_01_bronze",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "bronze"],
) as dag:
    bronze_download = BashOperator(
        task_id="bronze_download",
        bash_command=bash_command(""),
    )

    bronze_retry = BashOperator(
        task_id="bronze_retry",
        bash_command=bash_command(""),
    )

    trigger_silver_quality = TriggerDagRunOperator(
        task_id="trigger_silver_quality",
        trigger_dag_id="dag_02_silver_quality",
        wait_for_completion=True,
        poke_interval=30,
    )

    bronze_download >> bronze_retry >> trigger_silver_quality
