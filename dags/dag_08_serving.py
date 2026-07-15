"""Capa serving — FastAPI (disparo manual).

DAG 08 — Serving Layer (manual trigger).

Inicia el contenedor de FastAPI con la capa serving (historico + real-time +
fraud SSE). Este DAG NO se dispara automaticamente desde dag_05_gold — la capa
serving es un servicio de larga duracion, no un trabajo batch.

Disparo manual desde la UI de Airflow despues de que dag_05_gold y dag_07_gold_ml
hayan completado.

Prerrequisitos:
  - Redis corriendo (docker compose up redis -d)
  - Gold marts en data/gold/marts/
  - ML models en data/gold/models/ (opcional — serving funciona sin ellos)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_08_serving",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "serving"],
) as dag:
    BashOperator(
        task_id="serving_start",
        bash_command=bash_command("--serve"),
    )
