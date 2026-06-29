import argparse
import asyncio

from app.pipeline.bronze import BronzePipeline
from app.pipeline.silver import SilverPipeline
from app.profiling.profiling_pipeline import ProfilingPipeline
from app.utils.logger import Logger
from app.utils.settings import Settings


async def run_bronze_pipeline() -> None:
    logger = Logger()
    settings = Settings()

    logger.info("Iniciando pipeline de bronce para datos NY TLC")
    logger.info(f"Años configurados: {settings.config.datasets.years}")

    pipeline = BronzePipeline()
    await pipeline.run(settings.config.datasets)

    logger.info("Pipeline de bronce completado exitosamente")


async def run_profiling_pipeline() -> None:
    logger = Logger()
    settings = Settings()

    logger.info("Iniciando pipeline de profiling (8 dimensiones)")
    logger.info(f"Años configurados: {settings.config.datasets.years}")

    pipeline = ProfilingPipeline()
    await pipeline.run(settings.config.datasets)

    logger.info("Pipeline de profiling completado")
    logger.info("Reportes disponibles en data/profiling/")


def run_silver_quality() -> None:
    logger = Logger()
    settings = Settings()

    logger.info("Iniciando silver: calidad (correcciones de calidad de datos)")
    logger.info(f"Años configurados: {settings.config.datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_quality(settings.config.datasets)

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
    settings = Settings()

    logger.info("Iniciando silver: carga (modelo estrella - tablas de hechos)")
    logger.info(f"Años configurados: {settings.config.datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_load(settings.config.datasets)

    logger.info("Silver carga completado")
    logger.info("Tablas de hechos en data/silver/star/facts/")


def run_gold_pipeline(mode: str, only: list[str] | None) -> None:
    from app.pipeline.gold.gold_pipeline import GoldPipeline

    logger = Logger()
    settings = Settings()

    logger.info(f"Iniciando pipeline de oro (gold) | modo={mode}")
    logger.info(f"Años configurados: {settings.config.datasets.years}")

    pipeline = GoldPipeline(mode=mode, only=only)
    pipeline.run(settings.config)

    logger.info("Pipeline de oro completado")
    logger.info("Marts Power BI en data/gold/marts/")
    logger.info("Feature store ML en data/gold/ml/")
    logger.info("Auditoría en data/gold/audit.parquet")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL pipeline para NY TLC Trip Record Data"
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
    args = parser.parse_args()

    if args.silver:
        if args.silver == "quality":
            run_silver_quality()
        elif args.silver == "schema":
            run_silver_schema()
        elif args.silver == "load":
            run_silver_load()
    elif args.gold is not None:
        only = [s.strip() for s in args.only.split(",")] if args.only else None
        run_gold_pipeline(args.gold, only)
    elif args.profile:
        asyncio.run(run_profiling_pipeline())
    else:
        asyncio.run(run_bronze_pipeline())


if __name__ == "__main__":
    main()
