import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from app.utils import storage

# Spark lanza un proceso de Python por cada worker. Sin esto usa "python3" por
# defecto, que en Windows no existe (es python.exe) y falla con
# "CreateProcess error=5, Acceso denegado". Apuntamos al interprete actual
# (el del .venv cuando se ejecuta con `uv run`).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Requerido para Spark en Windows: hadoop.dll debe estar en PATH antes de que la JVM arranque
if hadoop_home := os.environ.get("HADOOP_HOME"):
    os.environ["hadoop.home.dir"] = hadoop_home
    hadoop_bin = str(Path(hadoop_home) / "bin")
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


class SparkClient:
    def __init__(self) -> None:

        hadoop_bin = str(Path(os.environ.get("HADOOP_HOME", "")) / "bin")

        # Recursos calibrados para esta maquina (Intel Ultra 9 285H: 16C/16T,
        # 32GB RAM, NVMe unico en C: con ~726GB libres). Configurables por env
        # para que Docker/WSL (VM mas chica) pueda bajarlos sin tocar codigo —
        # mismo patron que SPARK_LOCAL_DIR (abajo). Defaults = perfil Balanceado.
        driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "10g")
        master_cores = os.environ.get("SPARK_MASTER_CORES", "10")

        # Shuffle/spill en el disco del proyecto. En esta maquina data/ vive en el
        # NVMe de C: (rapido para shuffle, que puede crecer a ~5-15GB); ~726GB
        # libres, sin problema de cuota como tendria /tmp.
        # SPARK_LOCAL_DIR (env) permite override SOLO para Docker: dentro del
        # contenedor, data/ es un bind mount de Windows (I/O lenta via gRPC-FUSE)
        # y el shuffle debe ir a disco del contenedor (p.ej. /tmp/spark-temp, lo
        # fija docker-compose.yml). Sin la variable, comportamiento identico al
        # de siempre. El shuffle NUNCA va a S3, sea cual sea el backend.
        local_dir = os.environ.get("SPARK_LOCAL_DIR") or str(
            Path(__file__).resolve().parent.parent.parent / "data" / ".spark_temp"
        )
        os.makedirs(local_dir, exist_ok=True)

        builder = (
            SparkSession.builder.appName("Analisis_Presupuesto_MEF")
            # 10 de los 16 cores logicos (default; override via SPARK_MASTER_CORES).
            # El default local[*] (16 tareas en el mismo heap) provoca OutOfMemoryError
            # con datasets grandes (fhvhv ~20M filas: cada tarea de escaneo usa
            # ~0.6-1GB en buffers de descompresion). local[10] con heap de 10g es el
            # techo seguro: deja 6 hilos y ~22GB para el OS, los workers de Python de
            # PySpark y las etapas ML (pandas/sklearn/statsmodels, que corren en
            # proceso aparte). No subir cores sin subir el heap en la misma proporcion.
            .master(f"local[{master_cores}]")
            # 10g de heap (default; override via SPARK_DRIVER_MEMORY): 32GB totales dan
            # margen amplio para las window functions de silver sobre 20M filas sin
            # arriesgar OOM. En local mode driver y executor son la MISMA JVM, asi que
            # spark.executor.memory se ignora; solo cuenta driver.memory.
            .config("spark.driver.memory", driver_memory)
            .config("spark.executor.memory", driver_memory)
            # 128 particiones iniciales: tareas pequenas que caben en el heap y hacen
            # spill granular a disco. AQE (abajo) coalescea dinamicamente las que
            # queden pequenas, asi que 128 actua como techo para datasets grandes
            # (fhvhv ~20M filas) sin castigar a los chicos.
            .config("spark.sql.shuffle.partitions", "128")
            # AQE: replanifica en runtime segun estadisticas reales del shuffle —
            # coalescea particiones pequenas (objetivo ~64MB), parte joins con skew
            # y ajusta el plan fisico. No altera la logica ni los datos.
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
            # Codec Parquet zstd (nivel 3 por defecto) en vez de snappy: ~30-40%
            # menos disco en todas las escrituras (silver stage, star facts, gold
            # marts) con CPU despreciable en local. Aplica a todo df.write.parquet.
            .config("spark.sql.parquet.compression.codec", "zstd")
            # Nivel zstd 9. Medido sobre un mes real de yellow (3.4M filas):
            # nivel 19 = 38.6s / 51.9MB, nivel 9 = 6.2s / 53.2MB. Es decir, 19 solo
            # gana 2.5% de disco pero escribe 6x mas lento; con ~60 escrituras de
            # ~20M filas (fhvhv) el nivel 19 dominaba el tiempo del pipeline.
            # Nivel 9 es el equilibrio tiempo/tamano. Se propaga via conf hadoop.
            .config("spark.hadoop.parquet.compression.codec.zstd.level", "9")
            # Conversion vectorizada Arrow <-> pandas/Polars: menos copias y menos RAM.
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # Ocultar barra de progreso de Spark para limpiar los logs de Airflow
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.local.dir", local_dir)
            # Ruta de librerias nativas de Hadoop (winutils/hadoop.dll en Windows).
            # Se usa extraLibraryPath en lugar de "-Djava.library.path" dentro de
            # extraJavaOptions porque este ultimo elimina los backslashes de las
            # rutas de Windows (D:\...\bin -> D:...bin) y la JVM no carga hadoop.dll.
            .config("spark.driver.extraLibraryPath", hadoop_bin)
            .config("spark.executor.extraLibraryPath", hadoop_bin)
        )

        # STORAGE_BACKEND=s3: anade el conector hadoop-aws (esquema s3a) y lee
        # credenciales de las variables de entorno AWS_* estandar — nunca de
        # config.yaml. spark.local.dir se queda en disco local (arriba) aun con
        # backend S3: el shuffle/spill nunca debe ir a S3 (riesgo de OOM ya
        # documentado se agravaria con la latencia de red).
        if storage.get_backend() == "s3":
            builder = (
                builder.config(
                    "spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.787",
                )
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                )
                .config(
                    "spark.hadoop.fs.s3a.endpoint.region",
                    os.environ.get("AWS_REGION", "us-east-1"),
                )
            )

        self.spark = builder.getOrCreate()

    def get_session(self):
        return self.spark


def target_files(row_count: int, rows_per_file: int = 2_000_000, cap: int = 32) -> int:
    """Numero objetivo de archivos parquet para una escritura, acotado.

    Las window functions de silver disparan un shuffle, por lo que sin coalescer
    cada particion (stage / fact / mart de un mes) se escribia en decenas de
    archivos diminutos: mucho overhead de footer/diccionario por archivo y peor
    ratio de compresion (diccionarios no compartidos). Agrupar en ~2M filas/archivo
    reduce drasticamente el numero de archivos y mejora la compresion, sin forzar
    un unico archivo gigante en datasets grandes (cap=32). El count() que alimenta
    esto nunca es extra: siempre proviene de un conteo ya necesario (audit/log)."""
    if row_count <= 0:
        return 1
    return max(1, min((row_count + rows_per_file - 1) // rows_per_file, cap))
