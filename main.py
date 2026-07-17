import argparse
import asyncio
from copy import deepcopy

from app.pipeline.bronze import BronzePipeline
from app.pipeline.gold import GoldPipeline
from app.pipeline.gold_impl.ml.isolation_forest_model import IsolationForestModelPipeline
from app.pipeline.gold_impl.ml.kmodes_model import KModesModelPipeline
from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline
from app.pipeline.silver import SilverPipeline
from app.profiling.profiling_pipeline import ProfilingPipeline
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils.logger import Logger
from app.utils.settings import settings
from app.utils.globals import globals


async def run_bronze_pipeline(datasets_override: DatasetsConfig | None = None) -> None:
    logger = Logger()
    datasets = datasets_override or settings.datasets
    logger.info("Iniciando pipeline de bronce para datos NY TLC")
    logger.info(f"Años configurados: {datasets.years}")

    pipeline = BronzePipeline()
    await pipeline.run(datasets)

    logger.info("Pipeline de bronce completado exitosamente")


async def run_profiling_pipeline() -> None:
    logger = Logger()

    logger.info("Iniciando pipeline de profiling (8 dimensiones)")
    logger.info(f"Años configurados: {settings.datasets.years}")

    pipeline = ProfilingPipeline()
    await pipeline.run(settings.datasets)

    logger.info("Pipeline de profiling completado")
    logger.info("Reportes disponibles en data/profiling/")


def run_silver_quality(datasets_override: DatasetsConfig | None = None) -> None:
    logger = Logger()
    datasets = datasets_override or settings.datasets

    logger.info("Iniciando silver: calidad (correcciones de calidad de datos)")
    logger.info(f"Años configurados: {datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_quality(datasets)

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


def run_silver_load(datasets_override: DatasetsConfig | None = None) -> None:
    logger = Logger()
    datasets = datasets_override or settings.datasets

    logger.info("Iniciando silver: carga (modelo estrella - tablas de hechos)")
    logger.info(f"Años configurados: {datasets.years}")

    pipeline = SilverPipeline()
    pipeline.run_load(datasets)

    logger.info("Silver carga completado")
    logger.info("Tablas de hechos en data/silver/star/facts/")


def run_gold_pipeline(mode: str, only: list[str] | None, datasets_override: DatasetsConfig | None = None) -> None:
    logger = Logger()
    cfg = settings
    if datasets_override is not None:
        cfg = deepcopy(settings)
        cfg.datasets = datasets_override

    logger.info(f"Iniciando pipeline de oro (gold) | modo={mode}")
    logger.info(f"Años configurados: {cfg.datasets.years}")

    pipeline = GoldPipeline(mode=mode, only=only)
    pipeline.run(cfg)

    logger.info("Pipeline de oro completado")
    logger.info("Marts Power BI en data/gold/marts/")
    logger.info("Feature store ML en data/gold/ml/")
    logger.info("Auditoría en data/gold/audit.parquet")


def run_gold_ml_pipeline(
    which: str,
    datasets_override: DatasetsConfig | None = None,
    forecast_until_year: int | None = None,
) -> None:
    logger = Logger()
    cfg = settings
    if datasets_override is not None:
        cfg = deepcopy(settings)
        cfg.datasets = datasets_override
    if forecast_until_year is not None and which == "sarimax":
        if cfg is settings:
            cfg = deepcopy(settings)
        cfg.gold.sarimax.forecast_until_year = forecast_until_year

    if which == "isolation":
        logger.info("Iniciando Isolation Forest sobre ml_feat_isolation_fraud")
        logger.info(f"Años configurados: {cfg.datasets.years}")

        pipeline = IsolationForestModelPipeline(cfg)
        result = pipeline.run()

        if result >= 0:
            logger.info("Pipeline gold ML completado")
            logger.info("Scores en data/gold/ml/ml_isolation_fraud_scores/")
            logger.info("Modelos en data/gold/models/isolation_forest/")
    elif which == "kmodes":
        logger.info("Iniciando K-Modes sobre ml_feat_kmodes_trips")
        logger.info(f"Años configurados: {cfg.datasets.years}")

        pipeline = KModesModelPipeline(cfg)
        result = pipeline.run()

        if result >= 0:
            logger.info("Pipeline gold ML completado")
            logger.info("Labels en data/gold/ml/kmodes_model/labels_*/")
            logger.info("Modelos en data/gold/models/kmodes/")
    elif which == "sarimax":
        logger.info("Iniciando SARIMAX trip-count forecaster sobre ml_feat_arima_trips")
        logger.info(f"Años configurados: {cfg.datasets.years}")

        pipeline = SariMaxModelPipeline(cfg)
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


def run_serving() -> None:
    """Inicia la capa serving de FastAPI (historico + real-time + fraud SSE)."""
    logger = Logger()
    logger.info("Iniciando capa serving (FastAPI)")

    import uvicorn
    from app.serving.app import create_app

    app = create_app()
    uvicorn.run(
        app,
        host=settings.serving.host,
        port=settings.serving.port,
        log_level="info",
    )


def run_speed() -> None:
    """Motor de speed sin HTTP: EventProcessor + Redis, lee eventos de stdin (JSON lines)."""
    import asyncio
    import json
    import sys

    from app.speed.event_processor import EventProcessor
    from app.speed.pubsub import EventBus
    from app.speed.redis_client import RedisClient
    from app.speed.schema import RideEvent
    from app.speed.zone_lookup import ZoneLookup
    from app.speed.aggregation import RealtimeAggregator
    from app.speed.fraud_scorer import FraudScorer
    from app.speed.trip_profiler import TripProfiler
    from app.speed.ml_state import ModelLoader

    logger = Logger()
    logger.info("Iniciando motor de speed (stdin JSON lines)")

    async def _run():
        redis_client = RedisClient(settings.speed.redis_url, settings.speed.state_ttl_hours)
        await redis_client.connect()

        zone_lookup = ZoneLookup()
        zone_path = globals.project_root / "data/bronze/zone-lookup/zone-lookup-table.parquet"
        if zone_path.exists():
            zone_lookup.load(zone_path)

        event_bus = EventBus()
        processor = EventProcessor(zone_lookup, settings.speed)

        model_loader = ModelLoader()
        model_loader.load()

        aggregator = RealtimeAggregator(redis_client, settings.speed)
        fraud_scorer = FraudScorer(model_loader, settings.speed, redis_client)
        trip_profiler = TripProfiler(model_loader, redis_client)

        event_bus.subscribe(aggregator.on_event)
        event_bus.subscribe(fraud_scorer.on_event)
        event_bus.subscribe(trip_profiler.on_event)

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
                event = RideEvent(**raw)
                enriched = processor.process(event)
                if enriched is not None:
                    await event_bus.publish(enriched)
                    print(json.dumps({"status": "accepted", "trip_id": enriched.trip_id}))
                else:
                    print(json.dumps({"status": "rejected"}))
            except Exception as e:
                print(json.dumps({"status": "error", "message": str(e)}))

        await redis_client.close()

    asyncio.run(_run())


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
    parser.add_argument(
        "--forecast-until",
        type=int,
        default=None,
        help="Año hasta el cual pronosticar (solo con --gold-ml sarimax). "
        "Ej: --forecast-until 2027 pronostica por hora hasta 2027-12-31.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Iniciar capa serving (FastAPI + speed layer). "
        "Requiere Redis y datos gold en data/gold/",
    )
    parser.add_argument(
        "--speed",
        action="store_true",
        help="Iniciar solo el motor de speed (EventProcessor + Redis, sin HTTP). "
        "Util para tests de integracion con productores de eventos.",
    )

    # Download flag
    parser.add_argument(
        "--download",
        action="store_true",
        help="Descargar datos bronce (usa --cat/--year/--month para filtrar)",
    )

    # Target overrides (reusable across --download, --silver, --gold, --gold-ml)
    parser.add_argument(
        "--cat",
        nargs="+",
        default=None,
        choices=["yellow", "green", "fhv", "fhvhv"],
        help="Categoria(s): una o mas separadas por espacio (opcional: filtra categorias)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Año (opcional: filtra a un solo año)",
    )
    parser.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        default=None,
        help="Mes 1-12 (opcional: filtra a un solo mes; requiere --year). "
        "Ignorado si se usa --month-start/--month-end.",
    )
    parser.add_argument(
        "--month-start",
        type=int,
        choices=range(1, 13),
        default=None,
        help="Mes inicial 1-12 (opcional, requiere --year; usar con --month-end)",
    )
    parser.add_argument(
        "--month-end",
        type=int,
        choices=range(1, 13),
        default=None,
        help="Mes final 1-12 (opcional, requiere --year; usar con --month-start)",
    )
    args = parser.parse_args()

    # Build override if --cat/--year/--month/--month-start/--month-end provided
    datasets_override: DatasetsConfig | None = None
    if args.year is not None:
        cats = args.cat or globals.tlc_categories
        if args.month_end is not None:
            month_start = args.month_start or 1
            month_end = args.month_end or 12
            months = range(month_start, month_end + 1)
        elif args.month is not None:
            months = [args.month]
        else:
            months = range(1, 13)
        datasets_override = DatasetsConfig(
            years=[Module(category=cat, year=args.year, month=m) for cat in cats for m in months]
        )

    if args.download:
        asyncio.run(run_bronze_pipeline(datasets_override))
    elif args.all:
        run_full_pipeline()
    elif args.serve:
        run_serving()
    elif args.speed:
        run_speed()
    elif args.silver:
        if args.silver == "quality":
            run_silver_quality(datasets_override)
        elif args.silver == "schema":
            run_silver_schema()
        elif args.silver == "load":
            run_silver_load(datasets_override)
    elif args.gold is not None:
        only = [s.strip() for s in args.only.split(",")] if args.only else None
        run_gold_pipeline(args.gold, only, datasets_override)
    elif args.gold_ml is not None:
        run_gold_ml_pipeline(
            args.gold_ml, datasets_override, args.forecast_until
        )
    elif args.profile:
        asyncio.run(run_profiling_pipeline())
    else:
        asyncio.run(run_bronze_pipeline(datasets_override))


if __name__ == "__main__":
    main()
