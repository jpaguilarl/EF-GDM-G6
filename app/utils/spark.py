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
            bucket = os.environ.get("S3_BUCKET", "")
            builder = (
                builder.config(
                    # spark-hadoop-cloud aporta PathOutputCommitProtocol y
                    # BindingParquetOutputCommitter (el "pegamento" entre el
                    # commit protocol de Spark SQL y los committers S3A de
                    # hadoop-aws). Sin este jar el classpath NO tiene esas clases
                    # y el committer magic no engancha. Version = misma que Spark
                    # (4.1.2, Scala 2.13); hadoop-aws 3.4.x lo trae compatible.
                    "spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.787,"
                    "org.apache.spark:spark-hadoop-cloud_2.13:4.1.2",
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
                # --- Committer S3A "magic" -------------------------------------
                # Reemplaza el FileOutputCommitter basado en rename (en S3 el
                # rename es copy+delete NO atomico: dos jobs commiteando al mismo
                # prefijo se pisan -> FileNotFoundException en _temporary /
                # NoSuchKey 404 en copyFile). El magic committer NO usa
                # _temporary ni rename: cada task sube su parte con multipart
                # upload y el commit del job solo hace el POST final del multipart
                # (operacion atomica en S3). Elimina la carrera de raiz.
                .config("spark.hadoop.fs.s3a.committer.name", "magic")
                .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
                # Wiring de Spark SQL hacia el committer S3A. Sin estas dos clases,
                # df.write.parquet seguiria usando el committer de Hadoop por
                # defecto aunque fs.s3a.committer.name=magic.
                .config(
                    "spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
                )
                .config(
                    "spark.sql.parquet.output.committer.class",
                    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
                )
                # Conflicto de particion: replace = borra la particion destino
                # antes de commitear (equivalente a mode("overwrite") atomico por
                # directorio de mes). Coherente con la reescritura por mes de star.py.
                .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
                # --- Mitigacion de throttling S3 (503 "reduce your request rate")-
                # Dos jobs sobre el mismo prefijo duplicaban LIST/COPY/DELETE y
                # disparaban el limite por prefijo. Con el magic committer bajan
                # las operaciones, y estos reintentos con backoff absorben los 503
                # residuales sin abortar el job.
                .config("spark.hadoop.fs.s3a.retry.limit", "10")
                .config("spark.hadoop.fs.s3a.retry.throttle.interval", "1000ms")
                .config("spark.hadoop.fs.s3a.connection.maximum", "200")
                # --- Resiliencia de LECTURA S3A -------------------------------
                # Hadoop 3.4.2 arrastro el "analytics accelerator"
                # (analyticsaccelerator-s3) y su ruta de lectura vectorizada
                # (S3AInputStream.readVectored) corta la conexion sobre la red del
                # contenedor: "ConnectionClosedException: Premature end of
                # Content-Length delimited message body" -> FAILED_READ_FILE, que
                # tumbo mart_abc_xyz_zones al leer los facts fhvhv grandes. Forzar
                # el input stream clasico evita esa ruta. Timeouts mas holgados +
                # retry.interval absorben cortes transitorios de conexion.
                .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
                .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
                .config("spark.hadoop.fs.s3a.retry.interval", "1000ms")
            )
            # Habilitacion del magic committer a nivel bucket (ademas del global
            # de arriba). Se compone dinamicamente desde S3_BUCKET (nunca
            # hardcodeado) para que valga en cualquier bucket configurado.
            if bucket:
                builder = builder.config(
                    f"spark.hadoop.fs.s3a.bucket.{bucket}.committer.magic.enabled",
                    "true",
                )

        self.spark = builder.getOrCreate()

    def get_session(self):
        return self.spark


def use_default_committer(spark) -> None:
    """Revierte el commit protocol al default de Spark para esta sesion.

    SparkClient activa el committer S3A 'magic' a nivel de sesion (arregla la
    carrera de commit de silver, que escribe en paralelo al mismo prefijo). Pero
    el magic committer NO soporta ``partitionOverwriteMode=dynamic``
    (``IOException: PathOutputCommitter does not support dynamicPartitionOverwrite``,
    limitacion dura de hadoop-aws). La capa gold y los feature stores ML SI usan
    dynamic overwrite (reescriben una sola particion service_id/year/month sin
    borrar el mart entero), asi que deben volver al committer clasico
    (FileOutputCommitter basado en rename).

    Gold/ML no necesitan el magic committer: corren en un solo proceso y cada
    builder escribe a una ruta distinta, sin la concurrencia que rompia silver.
    En backend local esto es un no-op (el default ya es SQLHadoopMapReduceCommitProtocol).
    Son SQLConf runtime-settable, por eso se ajustan por-sesion aqui y no en el
    builder global de SparkClient (que silver necesita con magic)."""
    spark.conf.set(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
    )
    spark.conf.set(
        "spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter",
    )


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
