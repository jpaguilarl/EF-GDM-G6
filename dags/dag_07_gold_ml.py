"""Entrenamiento de modelos ML (K-Modes, Isolation Forest, SARIMAX).

DAG aparte del batch principal (dag_01..dag_06): el entrenamiento es pesado y
no siempre se quiere en cada corrida. Disparo manual desde la UI de Airflow.

Las tres tasks no tienen dependencia de orden entre si, pero se encadenan
secuencialmente a proposito: cada una lanza su propia SparkClient/proceso
pandas y el heap de 6g del contenedor ya esta comprometido por una corrida del
batch — no correrlas en paralelo (ver AGENTS.md).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, START_DATE, bash_command

with DAG(
    dag_id="dag_07_gold_ml",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    tags=["tlc", "gold-ml"],
) as dag:
    kmodes = BashOperator(
        task_id="gold_ml_kmodes",
        bash_command=bash_command("--gold-ml kmodes"),
    )

    isolation = BashOperator(
        task_id="gold_ml_isolation",
        bash_command=bash_command("--gold-ml isolation"),
    )

    sarimax = BashOperator(
        task_id="gold_ml_sarimax",
        bash_command=bash_command("--gold-ml sarimax"),
    )

    kmodes >> isolation >> sarimax
