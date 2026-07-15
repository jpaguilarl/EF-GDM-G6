"""Resolucion de storage local/S3, controlada por STORAGE_BACKEND (.env).

Unico punto de la capa de storage: el resto del codigo sigue componiendo rutas
con el operador ``/`` sobre ``globals.project_root`` exactamente igual que hoy
(ver app/utils/globals.py) — aqui solo se decide en que "raiz" aterriza esa
composicion. Para STORAGE_BACKEND=local la raiz es el mismo Path del proyecto
de siempre (cero cambio de comportamiento). Para STORAGE_BACKEND=s3, la raiz es
un S3Path que imita el subconjunto de pathlib.Path que este proyecto usa
(/, str, .parent, .exists, .mkdir, .stat) para que ningun call site necesite
reescribirse.
"""

import os
from pathlib import Path, PurePosixPath
from app.utils.settings import settings

LOCAL_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class _StatResult:
    def __init__(self, size: int) -> None:
        self.st_size = size


class S3Path:
    """Wrapper minimo tipo Path para una ubicacion s3://bucket/prefix/...

    S3 no tiene directorios reales (son solo prefijos de objetos), por eso
    .mkdir() es un no-op: el prefijo "existe" implicitamente al escribir el
    primer objeto bajo el. .exists()/.stat() usan s3fs (boto3 por debajo),
    que respeta las credenciales AWS_* del entorno sin que el codigo las toque.
    """

    def __init__(self, uri: str) -> None:
        self._uri = uri.rstrip("/")

    def __truediv__(self, other: str) -> "S3Path":
        return S3Path(f"{self._uri}/{PurePosixPath(str(other))}")

    def __str__(self) -> str:
        return self._uri

    def __fspath__(self) -> str:
        return self._uri

    def __repr__(self) -> str:
        return f"S3Path({self._uri!r})"

    @property
    def parent(self) -> "S3Path":
        head, _, _ = self._uri.rpartition("/")
        return S3Path(head)

    @property
    def name(self) -> str:
        return self._uri.rpartition("/")[-1]

    def _fs(self):
        import s3fs

        return s3fs.S3FileSystem(
            key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            client_kwargs={"region_name": os.environ.get("AWS_REGION")},
        )

    def exists(self) -> bool:
        return self._fs().exists(self._uri)

    def mkdir(self, parents: bool = True, exist_ok: bool = True) -> None:
        return None

    def stat(self) -> _StatResult:
        info = self._fs().info(self._uri)
        return _StatResult(size=info["size"])

    def glob(self, pattern: str) -> list["S3Path"]:
        matches = self._fs().glob(f"{self._uri}/{pattern}")
        return [S3Path(f"s3://{m}") for m in matches]

    def open(self, mode: str = "rb"):
        return self._fs().open(self._uri, mode)

    def write_text(self, data: str, encoding: str = "utf-8") -> None:
        with self._fs().open(self._uri, "w", encoding=encoding) as f:
            f.write(data)

    def read_text(self, encoding: str = "utf-8") -> str:
        with self._fs().open(self._uri, "r", encoding=encoding) as f:
            return f.read()


def get_backend() -> str:
    return settings.storage.backend


def get_root() -> Path | S3Path:
    if get_backend() == "s3":
        bucket = os.environ["S3_BUCKET"]
        prefix = os.environ.get("S3_PREFIX", "").strip("/")
        uri = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
        return S3Path(uri)
    # Ruta a traves de globals.project_root para que los tests puedan
    # redirectir todo el I/O con monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", tmp_path).
    # En produccion project_root == LOCAL_PROJECT_ROOT (mismo valor).
    from app.utils import globals as _globals_module  # lazy import para evitar circular import
    return _globals_module.globals.project_root


def data_path(*parts: str) -> Path | S3Path:
    """Ruta bajo el directorio de datos: data_path("bronze", "yellow") ->
    <raiz>/data/bronze/yellow (local o s3://bucket/prefix/data/bronze/yellow).

    Incluye SIEMPRE el segmento "data" para ser consistente con los call sites
    que componen ``globals.project_root / "data/silver/stage" / ...`` — ambas
    formas deben aterrizar bajo la misma raiz "data" en ambos backends.
    """
    path: Path | S3Path = get_root() / "data"
    for part in parts:
        path = path / part
    return path


def parquet_footer_readable(path: Path | S3Path) -> bool:
    """True si el footer parquet en ``path`` es legible (descarga completa).

    Local: pq.ParquetFile(path) directo. S3: pyarrow no entiende s3://, asi que
    se abre el objeto via s3fs (file-like) y se lee el footer desde ahi.
    """
    import pyarrow.parquet as pq

    try:
        if isinstance(path, S3Path):
            with path.open("rb") as f:
                pq.ParquetFile(f)
        else:
            pq.ParquetFile(path)
        return True
    except Exception:
        return False


def parquet_file(path: Path | S3Path):
    """pq.ParquetFile abierto para local o S3 (ver parquet_footer_readable)."""
    import pyarrow.parquet as pq

    if isinstance(path, S3Path):
        return pq.ParquetFile(path.open("rb"))
    return pq.ParquetFile(path)


def open_writable(path: Path | S3Path):
    """Context manager de escritura binaria, local o S3 (s3fs), sin bufferear
    el archivo completo en memoria (usado por la descarga por chunks)."""
    if isinstance(path, S3Path):
        return path.open("wb")
    path.parent.mkdir(parents=True, exist_ok=True)
    return open(path, "wb")


def for_spark(path: Path | S3Path | str) -> str:
    """String apta para spark.read/write.parquet: s3:// -> s3a:// si aplica.

    Hadoop/Spark usa el esquema s3a para el conector hadoop-aws; Polars,
    pyarrow y s3fs usan s3. Componer la ruta una sola vez (via globals.project_root
    o data_path) y pasarla por aqui SOLO en las llamadas a Spark evita mantener
    dos raices distintas.
    """
    s = str(path)
    if s.startswith("s3://"):
        return "s3a://" + s[len("s3://") :]
    return s
