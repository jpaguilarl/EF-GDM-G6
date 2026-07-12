import argparse
import asyncio

from app.pipeline.bronze import BronzePipeline
from app.pipeline.gold_impl.ml.isolation_forest_model import IsolationForestModelPipeline
from app.pipeline.gold_impl.ml.kmodes_model import KModesModelPipeline
from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline
from app.pipeline.silver import SilverPipeline
from app.profiling.profiling_pipeline import ProfilingPipeline
from app.utils.logger import Logger
from app.utils.settings import settings


async def run_bronze_pipeline() -> None:
    logger = Logger()
    logger.info("Iniciando pipeline de bronce para datos NY TLC")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = BronzePipeline()
    await pipeline.run(settings.datasets)

    logger.info("Pipeline de bronce completado exitosamente")


async def run_profiling_pipeline() -> None:
    logger = Logger()

    logger.info("Iniciando pipeline de profiling (8 dimensiones)")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = ProfilingPipeline()
    await pipeline.run(settings.datasets)

    logger.info("Pipeline de profiling completado")
    logger.info("Reportes disponibles en data/profiling/")


def run_silver_quality() -> None:
    logger = Logger()

    logger.info("Iniciando silver: calidad (correcciones de calidad de datos)")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_quality(settings.datasets)

    logger.info("Silver calidad completado")
    logger.info("Datos limpios en data/silver/stage/")
    logger.info("Datos rechazados en data/silver/reject/")
    logger.info("Auditoría en data/silver/audit.parquet")


def run_silver_schema() -> None:
    logger = Logger()

    logger.info("Iniciando silver: esquema (modelo estrella - dimensiones)")

    pipeline = SilverPipeline()
    pipeline.run_schema()

    logger.info("Silver esquema completado")
    logger.info("Dimensiones en data/silver/star/dims/")


def run_silver_load() -> None:
    logger = Logger()

    logger.info("Iniciando silver: carga (modelo estrella - tablas de hechos)")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_load(settings.datasets)

    logger.info("Silver carga completado")
    logger.info("Tablas de hechos en data/silver/star/facts/")


def run_gold_pipeline(mode: str, only: list[str] | None) -> None:
    from app.pipeline.gold import GoldPipeline

    logger = Logger()

    logger.info(f"Iniciando pipeline de oro (gold) | modo={mode}")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = GoldPipeline(mode=mode, only=only)
    pipeline.run(settings)

    logger.info("Pipeline de oro completado")
    logger.info("Marts Power BI en data/gold/marts/")
    logger.info("Feature store ML en data/gold/ml/")
    logger.info("Auditoría en data/gold/audit.parquet")


def run_gold_ml_pipeline(which: str) -> None:
    logger = Logger()

    if which == "isolation":
        logger.info("Iniciando Isolation Forest sobre ml_feat_isolation_fraud")
        logger.info(f"Años configurados: {settings.datasets.years}")

        pipeline = IsolationForestModelPipeline(settings)
        result = pipeline.run()

        if result >= 0:
            logger.info("Pipeline gold ML completado")
            logger.info("Scores en data/gold/ml/ml_isolation_fraud_scores/")
            logger.info("Modelos en data/gold/models/isolation_forest/")
    elif which == "kmodes":
        logger.info("Iniciando K-Modes sobre ml_feat_kmodes_trips")
        logger.info(f"Años configurados: {settings.datasets.years}")

        pipeline = KModesModelPipeline(settings)
        result = pipeline.run()

        if result >= 0:
            logger.info("Pipeline gold ML completado")
            logger.info("Labels en data/gold/ml/kmodes_model/labels_*/")
            logger.info("Modelos en data/gold/models/kmodes/")
    elif which == "sarimax":
        logger.info("Iniciando SARIMAX trip-count forecaster sobre ml_feat_arima_trips")
        logger.info(f"Años configurados: {settings.datasets.years}")

        pipeline = SariMaxModelPipeline(settings)
        result = pipeline.run()

        if result >= 0:
            logger.info("Pipeline gold ML completado")
            logger.info(
                "Predicciones en data/gold/ml/ml_sarimax_trips_forecast/")
            logger.info("Modelos en data/gold/models/sarimax/")


def _missing_bronze(datasets) -> list[str]:
    """Archivos bronce esperados que faltan o tienen footer parquet ilegible."""
    from app.schemas.settings_schema import Module
    from app.utils import storage
    from app.utils.globals import globals

    expected: list = []
    for year in datasets.years:
        if isinstance(year, int):
            for cat in globals.tlc_categories:
                for m in range(1, 13):
                    expected.append(storage.data_path("bronze", cat, f"{year}-{m:02d}.parquet"))
        elif isinstance(year, Module):
            for m in range(1, 13):
                expected.append(
                    storage.data_path("bronze", year.category, f"{year.year}-{m:02d}.parquet")
                )

    missing: list[str] = []
    for f in expected:
        if not storage.parquet_footer_readable(f):
            missing.append(str(f))
    return missing


def run_full_pipeline() -> None:
    """Pipeline completo end-to-end con un solo comando (replicacion).

    Orden: bronce -> verificacion de completitud de bronce (fail-loud, con un
    reintento: CloudFront responde 403 transitorios cuando la rafaga de
    descargas es grande y el pipeline de bronce NO falla por errores HTTP) ->
    silver calidad -> esquema -> carga -> gold incremental -> profiling.

    Profiling corre AL FINAL a proposito: es documentacion de solo lectura que
    no alimenta a silver ni a gold, asi los marts llegan horas antes.

    Todas las fases son idempotentes (ver CLAUDE.md): re-ejecutar este comando
    tras un corte o fallo retoma exactamente donde quedo sin repetir trabajo.
    Cualquier fallo de fase se propaga (exit != 0): no hay exito silencioso.
    """
    logger = Logger()
    datasets = settings.datasets

    logger.info("=== PIPELINE (1/7): bronce (descarga idempotente) ===")
    asyncio.run(run_bronze_pipeline())

    logger.info("=== PIPELINE (2/7): verificacion de completitud de bronce ===")
    missing = _missing_bronze(datasets)
    if missing:
        logger.warning(
            f"{len(missing)} archivos bronce faltantes/ilegibles; reintentando "
            "descarga (403 transitorio de CloudFront en rafagas grandes)"
        )
        asyncio.run(run_bronze_pipeline())
        missing = _missing_bronze(datasets)
    if missing:
        raise RuntimeError(
            f"Bronce incompleto tras reintento ({len(missing)} archivos): "
            + ", ".join(missing[:8])
            + ("..." if len(missing) > 8 else "")
        )
    logger.info("Bronce verificado: todos los archivos esperados legibles")

    logger.info("=== PIPELINE (3/7): silver calidad ===")
    run_silver_quality()

    logger.info("=== PIPELINE (4/7): silver esquema ===")
    run_silver_schema()

    logger.info("=== PIPELINE (5/7): silver carga ===")
    run_silver_load()

    logger.info("=== PIPELINE (6/7): gold (incremental) ===")
    run_gold_pipeline("incremental", None)

    logger.info("=== PIPELINE (7/7): profiling ===")
    asyncio.run(run_profiling_pipeline())

    logger.info("=== PIPELINE COMPLETO: 7/7 fases OK ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL pipeline para NY TLC Trip Record Data"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Pipeline completo: bronce -> verificacion -> silver -> esquema -> "
        "carga -> gold incremental -> profiling. Idempotente y reanudable.",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Ejecutar pipeline de profiling en lugar de bronze",
    )
    parser.add_argument(
        "--silver",
        nargs="?",
        const="quality",
        default=None,
        choices=["quality", "schema", "load"],
        help="Ejecutar pipeline silver: quality (default), schema, load",
    )
    parser.add_argument(
        "--gold",
        nargs="?",
        const="full",
        default=None,
        choices=["full", "incremental"],
        help="Ejecutar capa gold: full (default) o incremental",
    )
    parser.add_argument(
        "--only",
        default=None,
        help="Lista separada por comas de marts/features a construir (solo con --gold)",
    )
    parser.add_argument(
        "--gold-ml",
        nargs="?",
        const="kmodes",
        default=None,
        choices=["kmodes", "isolation", "sarimax"],
        help="Entrenar modelos ML: kmodes (default), isolation, o sarimax",
    )
    args = parser.parse_args()

    if args.all:
        run_full_pipeline()
    elif args.silver:
        if args.silver == "quality":
            run_silver_quality()
        elif args.silver == "schema":
            run_silver_schema()
        elif args.silver == "load":
            run_silver_load()
    elif args.gold is not None:
        only = [s.strip() for s in args.only.split(",")] if args.only else None
        run_gold_pipeline(args.gold, only)
    elif args.gold_ml is not None:
        run_gold_ml_pipeline(args.gold_ml)
    elif args.profile:
        asyncio.run(run_profiling_pipeline())
    else:
        asyncio.run(run_bronze_pipeline())


if __name__ == "__main__":
    main()
