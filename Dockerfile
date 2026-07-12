# Imagen custom sobre Airflow: anade JDK (PySpark 4.x requiere Java 17+) y `uv`
# para correr el CLI existente (`uv run main.py ...`) tal cual, en un venv de
# proyecto propio e independiente del venv interno de Airflow. Spark corre
# embebido en el proceso de la tarea (LocalExecutor) — no hay contenedor Spark
# separado, tal como se documento en el plan de refactor.
FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Binario standalone de uv (no depende del venv de Airflow).
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /usr/local/bin/uv

USER airflow

WORKDIR /opt/airflow/project

# Instalar dependencias primero (cache de capa): solo se reinstala si
# pyproject.toml/uv.lock cambian, no en cada cambio de codigo de app/.
# NOTA: los extras "s3" y "jupyter" se anadieron a pyproject.toml; si uv.lock
# no los tiene todavia, `uv sync --frozen` FALLA a proposito (lockfile
# desactualizado). Correr `uv lock` en el host una vez y commitear el lock
# antes de `docker compose build`.
COPY --chown=airflow:root pyproject.toml uv.lock ./
# Extras s3 + jupyter siempre instalados en esta imagen: es la unica imagen
# usada tanto por los servicios airflow-* (necesitan s3fs solo si
# STORAGE_BACKEND=s3, pero instalarlo siempre es mas simple que un segundo
# Dockerfile) como por el servicio jupyter (necesita jupyterlab).
RUN uv sync --frozen --no-dev --extra s3 --extra jupyter

# Pre-descargar dependencias Maven (JARs) de Spark en la imagen de Docker.
# Esto hace que PySpark descargue hadoop-aws y aws-java-sdk a ~/.ivy2/cache 
# durante el `docker build`. Asi, cuando los DAGs corran, encontraran las
# librerias en cache inmediatamente y no usaran internet, evitando colisiones.
RUN uv run python -c "from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.787').getOrCreate().stop()"

COPY --chown=airflow:root . .

ENV PYTHONPATH=/opt/airflow/project
