"""Fase 4/6: silver carga (tablas de hechos del modelo estrella).

Requiere que dag_03_silver_schema haya completado (dimensiones existentes).
Al terminar, dispara gold (incremental).
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DAG_KWARGS, bash_command, trigger_next

with DAG(
    dag_id="dag_04_silver_load",
    tags=["tlc", "silver"],
    # DAG_KWARGS ya trae max_active_runs=1: un solo DagRun activo a la vez, para
    # que un backfill/disparo manual no solape dos corridas escribiendo a la
    # misma ruta de facts en S3.
    **DAG_KWARGS,
) as dag:
    silver_load = BashOperator(
        task_id="silver_load",
        bash_command=bash_command("--silver load"),
        # Prioridad maxima: una sola instancia concurrente de esta tarea. Si un
        # try queda zombie (commit lento en S3 hizo perder el heartbeat) y
        # Airflow lanza el retry, este limite impide que el retry solape con el
        # zombie escribiendo al mismo prefijo (la carrera de commit que rompio
        # 2024-02..06). Alternativa equivalente sin tocar codigo: un pool con 1
        # slot (`airflow pools set spark_serial 1 "un spark a la vez"`) y
        # pool="spark_serial"; se usa max_active_tis_per_dag para no depender de
        # crear el pool a mano.
        max_active_tis_per_dag=1,
    )

    trigger_gold = trigger_next("trigger_gold", "dag_05_gold")

    silver_load >> trigger_gold
