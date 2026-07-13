from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.serving.merged_view import MergedViewReader
from app.serving.query_engine import PolarsQueryEngine
from app.serving.routes import admin, health, historic, realtime
from app.speed.event_processor import EventProcessor
from app.speed.fraud_scorer import FraudScorer
from app.speed.ingest import router as ingest_router
from app.speed.ml_state import ModelLoader
from app.speed.pubsub import EventBus
from app.speed.redis_client import RedisClient
from app.speed.trip_profiler import TripProfiler
from app.speed.zone_lookup import ZoneLookup
from app.utils.globals import globals
from app.utils.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
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

    fraud_scorer = FraudScorer(model_loader, settings.speed, redis_client)
    trip_profiler = TripProfiler(model_loader, redis_client)

    event_bus.subscribe(fraud_scorer.on_event)
    event_bus.subscribe(trip_profiler.on_event)

    app.state.redis = redis_client
    app.state.event_bus = event_bus
    app.state.processor = processor
    app.state.model_loader = model_loader
    app.state.merged_reader = MergedViewReader(
        engine=app.state.engine,
        redis=redis_client,
        block_minutes=settings.gold.supply_demand.block_minutes,
    )

    yield

    await redis_client.close()


def create_app() -> FastAPI:
    app = FastAPI(title="NY TLC Serving Layer", version="0.1.0", lifespan=lifespan)
    engine = PolarsQueryEngine(
        marts_dir=globals.project_root / "data/gold/marts",
        cache_ttl=settings.serving.query_cache_ttl_seconds,
    )
    app.state.engine = engine
    app.include_router(historic.router)
    app.include_router(realtime.router)
    app.include_router(health.router)
    app.include_router(ingest_router)
    app.include_router(admin.router)
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.serving.app:app",
        host=settings.serving.host,
        port=settings.serving.port,
    )
