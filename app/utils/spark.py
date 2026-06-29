import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

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

        # Usar disco local (~282G libres) en vez de /tmp (tmpfs con cuota ~6GB)
        # para evitar java.io.IOException por cuota de disco al hacer shuffle.
        local_dir = str(Path(__file__).resolve().parent.parent.parent / "data" / ".spark_temp")
        os.makedirs(local_dir, exist_ok=True)

        self.spark = (
            SparkSession.builder.appName("Analisis_Presupuesto_MEF")
            # Limitar la concurrencia a 4 cores. El default local[*] usa los 12 cores
            # = 12 tareas compartiendo el mismo heap; con datasets grandes (fhvhv ~20M
            # filas) eso provoca OutOfMemoryError. Menos tareas = mas heap por tarea.
            .master("local[4]")
            # 6g de heap: hay ~8GB libres y las window functions de silver sobre 20M
            # filas necesitan margen. En local mode driver y executor son la MISMA JVM,
            # asi que spark.executor.memory se ignora; solo cuenta driver.memory.
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "6g")
            # 64 particiones: tareas pequenas (~filas/64) que caben en el heap y hacen
            # spill granular a disco. Bajar esto (p.ej. a 8) con datasets grandes hace
            # que cada tarea procese demasiadas filas y reviente la JVM.
            .config("spark.sql.shuffle.partitions", "64")
            # Conversion vectorizada Arrow <-> pandas/Polars: menos copias y menos RAM.
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.local.dir", local_dir)
            # Ruta de librerias nativas de Hadoop (winutils/hadoop.dll en Windows).
            # Se usa extraLibraryPath en lugar de "-Djava.library.path" dentro de
            # extraJavaOptions porque este ultimo elimina los backslashes de las
            # rutas de Windows (D:\...\bin -> D:...bin) y la JVM no carga hadoop.dll.
            .config("spark.driver.extraLibraryPath", hadoop_bin)
            .config("spark.executor.extraLibraryPath", hadoop_bin)
            .getOrCreate()
        )

    def get_session(self):
        return self.spark
