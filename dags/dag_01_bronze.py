"""Fase 1/6: descarga bronce (zone-lookup + parquet mensuales por categoria).

Idempotente (skip si el footer parquet ya es legible). La segunda descarga es
un reintento barato: CloudFront responde 403 transitorios en rafagas grandes,
pero un archivo ya valido se omite instantaneamente (mismo patron que
run_full_pipeline en main.py). Al terminar, dispara silver quality.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command, trigger_next

with DAG(
    dag_id="dag_01_bronze",
    tags=["tlc", "bronze"],
    **DAG_KWARGS,
) as dag:
    bronze_download = BashOperator(
        task_id="bronze_download",
        bash_command=bash_command(""),
    )

    bronze_retry = BashOperator(
        task_id="bronze_retry",
        bash_command=bash_command(""),
    )

    trigger_silver_quality = trigger_next(
        "trigger_silver_quality", "dag_02_silver_quality"
    )

    bronze_download >> bronze_retry >> trigger_silver_quality
