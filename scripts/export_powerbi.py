"""Consolida la capa gold particionada en un parquet unico por dataset para Power BI.

Los datasets gold se escriben particionados estilo Hive
(``service_id=yellow/year=2025/month=1/part-*.parquet``). Las columnas de
particion viven SOLO en el nombre de la carpeta, no dentro del parquet: al
concatenar los archivos a mano se pierden silenciosamente. Este script las
rematerializa como columnas reales antes de escribir.

La escritura es en streaming (batch a batch), no carga el dataset en memoria: los
feature stores a grano de viaje superan las 100M de filas.

Uso:
    uv run scripts/export_powerbi.py                  # todo -> data/powerbi/
    uv run scripts/export_powerbi.py --only mart_demand_volume,dim_zone_gold
    uv run scripts/export_powerbi.py --skip-heavy     # omite feature stores trip-grain
    uv run scripts/export_powerbi.py --compression zstd
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.utils.globals import globals  # noqa: E402
from app.utils.logger import Logger  # noqa: E402

GOLD_DIR = globals.project_root / "data" / "gold"
OUT_DIR = globals.project_root / "data" / "powerbi"

# Snappy por defecto, no zstd (la convencion de storage del proyecto): el conector
# Parquet de Power Query soporta snappy en cualquier version, mientras que zstd
# depende de la build de Power BI Desktop. El export prioriza compatibilidad;
# --compression zstd queda disponible si el destino lo soporta.
DEFAULT_COMPRESSION = "snappy"

# Filas acumuladas por row group. ~1M mantiene los row groups utiles para el
# predicate pushdown de Power BI sin inflar la memoria del writer.
ROW_GROUP_ROWS = 1_000_000
SCAN_BATCH_ROWS = 131_072

# Feature stores a grano de viaje: insumo de entrenamiento ML, no de tablero.
HEAVY = {"ml_feat_isolation_fraud", "ml_feat_kmodes_trips"}


def _data_files(root: Path) -> list[Path]:
    """Parquets reales bajo `root`.

    Descarta cualquier ruta con un segmento que empiece en '.' o '_', igual que el
    `ignore_prefixes` por defecto de pyarrow: excluye `_SUCCESS`, los `.crc` de
    Spark y los directorios de staging `_tmp_*` que deja isolation_forest_model
    (contienen una copia duplicada de los scores). Debe coincidir con lo que
    realmente lee `ds.dataset()`, o el esquema unificado describiria archivos que
    el scan nunca ve.
    """
    return [
        f
        for f in root.rglob("*")
        if f.is_file()
        and ".parquet" in f.name
        and not any(p.startswith((".", "_")) for p in f.relative_to(root).parts)
    ]


def _unified_schema(files: list[Path]) -> tuple[pa.Schema, list[str]]:
    """Union de los esquemas de todos los archivos + columnas con drift.

    ``ds.dataset()`` infiere el esquema del PRIMER fragmento: una columna que solo
    aparece en algunos meses (p.ej. ``cbd_congestion_fee``, que existe desde 2025)
    se perderia. Unificar sobre todos los archivos lo evita.
    """
    schemas = [pq.read_schema(f) for f in files]
    unified = pa.unify_schemas(schemas, promote_options="permissive")
    baseline = set(schemas[0].names)
    drift = sorted(set(unified.names) - baseline)
    return unified, drift


def _partition_keys(root: Path, files: list[Path]) -> list[str]:
    """Claves `k=v` presentes en los nombres de carpeta bajo `root`."""
    keys: list[str] = []
    for f in files:
        for segment in f.relative_to(root).parts[:-1]:
            if "=" in segment:
                key = segment.split("=", 1)[0]
                if key not in keys:
                    keys.append(key)
    return keys


def _export_dataset(src: Path, dst: Path, compression: str) -> tuple[int, list[str]]:
    """Lee `src` (directorio) y escribe un parquet unico con `dst`.

    Cubre los tres layouts que produce el gold:
    - Hive puro (marts, feature stores): las claves solo existen en la carpeta y
      hay que rematerializarlas como columnas.
    - Claves ya dentro del archivo (ml_isolation_fraud_scores,
      ml_sarimax_trips_forecast): se lee plano. Aplicar particionado Hive aqui
      choca contra la columna homonima del archivo (`ratecode_id` int64 vs int32,
      `borough` large_string vs string).
    - Sin particionar (dims): directorio `dim_x.parquet/part-*.parquet`.
    """
    files = _data_files(src)
    if not files:
        raise FileNotFoundError(f"sin parquets en {src}")

    file_schema, drift = _unified_schema(files)

    keys = _partition_keys(src, files)
    missing = [k for k in keys if k not in file_schema.names]
    present = [k for k in keys if k in file_schema.names]
    if missing and present:
        # Nadie lo produce hoy; si aparece, fallar antes que adivinar cual gana.
        raise ValueError(
            f"particionado mixto en {src.name}: {present} dentro del archivo y "
            f"{missing} solo en la carpeta"
        )

    if missing:
        # infer_dictionary=False -> columnas de particion con tipo plano (no
        # diccionario), que es lo que Power BI espera ver como columna normal.
        partitioning = ds.HivePartitioning.discover(infer_dictionary=False)
        dataset = ds.dataset(src, format="parquet", partitioning=partitioning)
        # El esquema del dataset trae las columnas de particion; se combina con el
        # esquema unificado de los archivos para no perder columnas ausentes en el
        # primer fragmento.
        schema = pa.unify_schemas(
            [dataset.schema, file_schema], promote_options="permissive"
        )
        dataset = ds.dataset(src, format="parquet", partitioning=partitioning, schema=schema)
    else:
        schema = file_schema
        dataset = ds.dataset(src, format="parquet", partitioning=None, schema=schema)

    writer = pq.ParquetWriter(dst, schema, compression=compression)
    rows = 0
    pending: list[pa.RecordBatch] = []
    pending_rows = 0
    try:
        for batch in dataset.scanner(batch_size=SCAN_BATCH_ROWS).to_batches():
            if batch.num_rows == 0:
                continue
            pending.append(batch)
            pending_rows += batch.num_rows
            rows += batch.num_rows
            if pending_rows >= ROW_GROUP_ROWS:
                writer.write_table(pa.Table.from_batches(pending, schema=schema))
                pending, pending_rows = [], 0
        if pending:
            writer.write_table(pa.Table.from_batches(pending, schema=schema))
    finally:
        writer.close()

    return rows, drift


def _copy_single(src: Path, dst: Path) -> int:
    """Origen que ya es un parquet unico: copia directa."""
    shutil.copyfile(src, dst)
    return pq.ParquetFile(dst).metadata.num_rows


def _kmodes_groups(root: Path) -> dict[str, list[tuple[Path, str]]]:
    """Agrupa `kmodes_model/` por artefacto.

    El directorio mezcla 4 datasets distintos (tuning/centers/labels/profiles), cada
    uno con su propio sufijo de particion (``centers_service_id=yellow``), asi que
    no es un dataset Hive valido y hay que separarlos a mano.
    """
    groups: dict[str, list[tuple[Path, str]]] = {}
    for d in sorted(root.iterdir()):
        if not d.is_dir() or "_service_id=" not in d.name:
            continue
        artifact, service_id = d.name.split("_service_id=", 1)
        if not _data_files(d):
            continue
        groups.setdefault(artifact, []).append((d, service_id))
    return groups


def _export_kmodes(
    artifact: str, parts: list[tuple[Path, str]], dst: Path, compression: str
) -> int:
    """Concatena los artefactos kmodes por servicio, agregando `service_id`."""
    tables = []
    for d, service_id in parts:
        t = ds.dataset(d, format="parquet").to_table()
        # `labels` ya trae service_id dentro del archivo; no duplicar la columna.
        if "service_id" not in t.column_names:
            t = t.append_column(
                "service_id", pa.array([service_id] * t.num_rows, pa.string())
            )
        tables.append(t)
    table = pa.concat_tables(tables, promote_options="permissive")
    pq.write_table(table, dst, compression=compression)
    return table.num_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="lista separada por comas de datasets a exportar")
    parser.add_argument(
        "--skip-heavy",
        action="store_true",
        help="omite los feature stores a grano de viaje (>100M filas)",
    )
    parser.add_argument("--compression", default=DEFAULT_COMPRESSION)
    args = parser.parse_args()

    logger = Logger()

    if not GOLD_DIR.exists():
        logger.error(f"No existe la capa gold en {GOLD_DIR}. Ejecuta primero --gold.")
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # (nombre, ruta origen, tipo)
    jobs: list[tuple[str, Path, str]] = []
    for sub in ("marts", "ml"):
        base = GOLD_DIR / sub
        if not base.exists():
            continue
        for d in sorted(base.iterdir()):
            if not d.is_dir():
                continue
            if d.name == "kmodes_model":
                for artifact, parts in _kmodes_groups(d).items():
                    jobs.append((f"kmodes_{artifact}", d, "kmodes"))
                continue
            jobs.append((d.name, d, "partitioned"))
    # Spark escribe las dims como directorio `dim_x.parquet/`, no como archivo.
    for f in sorted((GOLD_DIR / "dims").iterdir()):
        if ".parquet" not in f.name:
            continue
        name = f.name.removesuffix(".parquet")
        jobs.append((name, f, "partitioned" if f.is_dir() else "single"))

    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        unknown = wanted - {j[0] for j in jobs}
        if unknown:
            logger.error(f"Datasets desconocidos: {sorted(unknown)}")
            return 1
        jobs = [j for j in jobs if j[0] in wanted]
    if args.skip_heavy:
        jobs = [j for j in jobs if j[0] not in HEAVY]

    logger.info(f"Exportando {len(jobs)} datasets gold -> {OUT_DIR} ({args.compression})")

    results: list[tuple[str, int, float]] = []
    failures: list[tuple[str, Exception]] = []
    for name, src, kind in jobs:
        dst = OUT_DIR / f"{name}.parquet"
        t0 = time.perf_counter()
        try:
            if kind == "single":
                rows = _copy_single(src, dst)
            elif kind == "kmodes":
                artifact = name.removeprefix("kmodes_")
                rows = _export_kmodes(
                    artifact, _kmodes_groups(src)[artifact], dst, args.compression
                )
            else:
                rows, drift = _export_dataset(src, dst, args.compression)
                if drift:
                    logger.warning(
                        f"  {name}: columnas ausentes en el primer fragmento, "
                        f"recuperadas por unificacion de esquema: {drift}"
                    )
        except Exception as exc:  # fail loud, pero sin abortar el resto del export
            logger.exception(f"  {name}: FALLO ({exc})")
            failures.append((name, exc))
            dst.unlink(missing_ok=True)
            continue
        elapsed = time.perf_counter() - t0
        mb = dst.stat().st_size / 1024 / 1024
        results.append((name, rows, mb))
        logger.info(f"  {name}: {rows:,} filas, {mb:,.1f} MB, {elapsed:,.1f}s")

    total_rows = sum(r[1] for r in results)
    total_mb = sum(r[2] for r in results)
    logger.info(
        f"Listo: {len(results)} parquets, {total_rows:,} filas, {total_mb:,.1f} MB en {OUT_DIR}"
    )
    if failures:
        logger.error(f"Fallaron {len(failures)}: {[f[0] for f in failures]}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
