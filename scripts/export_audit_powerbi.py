"""Exporta la traza de auditoria (bronze/silver/gold/ml) lista para Power BI.

Las `audit.parquet` del pipeline son un log de tipo append: cada re-corrida agrega
una fila en vez de reemplazar, los timestamps se guardan como String y la
categoria/anio/mes viven embutidos en `source_file`. Un dashboard que las consuma
crudas suma las re-corridas (infla bronze un 43%) y no puede segmentar ni hacer
time intelligence.

Este export no descarta historial: marca `es_ultima_corrida` para que el tablero
filtre el estado vigente y, si quiere, muestre las re-corridas aparte.

Salida -> data/powerbi_audit/
    audit_bronze.parquet    una fila por descarga (+ re-corridas)
    audit_silver.parquet    una fila por limpieza (+ re-corridas), con % de rechazo
    audit_gold.parquet      una fila por builder ejecutado, con filas_en_disco
    audit_ml.parquet        una fila por modelo entrenado + hiperparametros
    audit_linaje.parquet    cobertura y calidad por categoria x anio x mes

Uso:
    uv run scripts/export_audit_powerbi.py
"""

import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.utils.globals import globals  # noqa: E402
from app.utils.logger import Logger  # noqa: E402

ROOT = globals.project_root
OUT_DIR = ROOT / "data" / "powerbi_audit"
GOLD_EXPORT = ROOT / "data" / "powerbi"

# Los audit guardan la ruta con el separador del SO que corrio el pipeline:
# bronze quedo con '\' (Windows) y silver con '/'. Sin normalizar, el join del
# linaje da cero filas.

# El zone-lookup es un artefacto de bronze (la tabla de 265 zonas), no datos de
# viajes: no pasa por silver, asi que queda fuera del linaje.
ZONE_LOOKUP = "zone-lookup"


def _norm_path(col: str) -> pl.Expr:
    return pl.col(col).str.replace_all(r"\\", "/")


def _parse_source(df: pl.DataFrame, col: str = "source_file") -> pl.DataFrame:
    """Extrae categoria/anio/mes de la ruta (no existen como columnas propias)."""
    return df.with_columns(
        [
            _norm_path(col).str.extract(r"bronze/([^/]+)/").alias("categoria"),
            _norm_path(col).str.extract(r"/(\d{4})-\d{2}\.parquet$").cast(pl.Int32).alias("anio"),
            _norm_path(col).str.extract(r"/\d{4}-(\d{2})\.parquet$").cast(pl.Int32).alias("mes"),
            _norm_path(col).alias("source_file"),
        ]
    )


def _timestamps(df: pl.DataFrame, start: str, end: str | None = None) -> pl.DataFrame:
    """String ISO -> Datetime real, + duracion. Sin esto Power BI los trata como texto."""
    out = df.with_columns(pl.col(start).str.to_datetime(time_unit="us", strict=False).alias("inicio"))
    if end is not None:
        out = out.with_columns(
            pl.col(end).str.to_datetime(time_unit="us", strict=False).alias("fin")
        ).with_columns(
            ((pl.col("fin") - pl.col("inicio")).dt.total_milliseconds() / 1000.0)
            .round(3)
            .alias("duracion_segundos")
        )
    return out.drop([c for c in (start, end) if c is not None and c in out.columns])


def _run_flags(df: pl.DataFrame, key: str) -> pl.DataFrame:
    """Numera las re-corridas por `key` y marca la vigente.

    No se deduplica: el historial de re-corridas es parte de la auditoria. El
    tablero filtra `es_ultima_corrida` para el estado actual.
    """
    return df.with_columns(
        [
            pl.col("inicio").rank("ordinal").over(key).cast(pl.Int32).alias("n_corrida"),
            (pl.col("inicio") == pl.col("inicio").max().over(key)).alias("es_ultima_corrida"),
        ]
    ).with_columns(pl.len().over(key).cast(pl.Int32).alias("total_corridas"))


def _rows_on_disk(name: str) -> int | None:
    """Filas reales del dataset exportado.

    El gold audit guarda `rowcount_output` = filas ESCRITAS en esa corrida, que en
    modo incremental es 0 cuando las particiones ya existian. Sin esta columna el
    tablero muestra marts "vacios" que en realidad estan completos.
    """
    f = GOLD_EXPORT / f"{name}.parquet"
    if not f.exists():
        return None
    return pq.ParquetFile(f).metadata.num_rows


def main() -> int:
    logger = Logger()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- bronze ---------------------------------------------------------------
    b = pl.read_parquet(ROOT / "data/bronze/audit.parquet")
    b = _parse_source(b)
    b = _timestamps(b, "start_timestamp", "end_timestamp")
    b = _run_flags(b, "source_file")
    b = b.with_columns(
        [
            (pl.col("bytecount") / 1024 / 1024).round(2).alias("mb_descargados"),
            # El regex extrae 'zone-lookup' como categoria, no null: comparar contra
            # la constante, no chequear nulidad.
            (pl.col("categoria") == ZONE_LOOKUP).alias("es_zone_lookup"),
        ]
    ).sort(["categoria", "anio", "mes"])
    b.write_parquet(OUT_DIR / "audit_bronze.parquet")
    logger.info(f"  audit_bronze: {b.height} filas ({b['source_file'].n_unique()} archivos)")

    # --- silver ---------------------------------------------------------------
    s = pl.read_parquet(ROOT / "data/silver/audit.parquet")
    s = _parse_source(s)
    s = _timestamps(s, "start_timestamp", "end_timestamp")
    s = _run_flags(s, "source_file")
    s = s.with_columns(
        pl.when(pl.col("rowcount_bronze") > 0)
        .then(pl.col("rowcount_quarantined") / pl.col("rowcount_bronze") * 100)
        .otherwise(None)
        .round(4)
        .alias("pct_rechazo")
    ).sort(["categoria", "anio", "mes"])
    s.write_parquet(OUT_DIR / "audit_silver.parquet")
    logger.info(f"  audit_silver: {s.height} filas ({s['source_file'].n_unique()} archivos)")

    # --- gold -----------------------------------------------------------------
    g = pl.read_parquet(ROOT / "data/gold/audit.parquet")
    g = _timestamps(g, "start_timestamp", "end_timestamp")
    g = _run_flags(g, "mart_name")
    g = g.with_columns(
        pl.col("mart_name")
        .map_elements(_rows_on_disk, return_dtype=pl.Int64)
        .alias("filas_en_disco")
    ).sort(["mart_name", "n_corrida"])
    g.write_parquet(OUT_DIR / "audit_gold.parquet")
    logger.info(f"  audit_gold: {g.height} filas ({g['mart_name'].n_unique()} builders)")

    # --- ml -------------------------------------------------------------------
    m = pl.read_parquet(ROOT / "data/gold/ml/audit.parquet")
    m = _timestamps(m, "started_at")
    m = _run_flags(m, "pipeline").sort("pipeline")
    m.write_parquet(OUT_DIR / "audit_ml.parquet")
    logger.info(f"  audit_ml: {m.height} filas ({m['pipeline'].n_unique()} modelos)")

    # --- linaje: cobertura + calidad por categoria x anio x mes ---------------
    # Solo la corrida vigente de cada archivo; sumar el historial inflaria bronze.
    bl = b.filter(pl.col("es_ultima_corrida") & ~pl.col("es_zone_lookup")).select(
        ["source_file", "categoria", "anio", "mes", "rowcount", "bytecount"]
    )
    sl = s.filter(pl.col("es_ultima_corrida")).select(
        ["source_file", "rowcount_bronze", "rowcount_quality", "rowcount_quarantined"]
    )
    lin = (
        bl.join(sl, on="source_file", how="left")
        .rename({"rowcount": "filas_descargadas"})
        .with_columns(
            [
                (pl.col("bytecount") / 1024 / 1024).round(2).alias("mb_descargados"),
                pl.when(pl.col("rowcount_bronze") > 0)
                .then(pl.col("rowcount_quarantined") / pl.col("rowcount_bronze") * 100)
                .otherwise(None)
                .round(4)
                .alias("pct_rechazo"),
                pl.col("rowcount_quality").is_null().alias("falta_en_silver"),
            ]
        )
        .sort(["categoria", "anio", "mes"])
    )
    lin.write_parquet(OUT_DIR / "audit_linaje.parquet")

    matched = lin.filter(pl.col("rowcount_quality").is_not_null()).height
    logger.info(f"  audit_linaje: {lin.height} filas ({matched} con match en silver)")
    if matched < lin.height:
        logger.warning(f"  {lin.height - matched} archivos de bronze sin entrada en silver")

    logger.info(f"Auditoria exportada -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
