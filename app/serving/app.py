from __future__ import annotations

from fastapi import FastAPI

from app.serving.query_engine import PolarsQueryEngine
from app.serving.routes import health, historic, realtime
from app.utils.globals import globals
from app.utils.settings import settings


def create_app() -> FastAPI:
    app = FastAPI(title="NY TLC Serving Layer", version="0.1.0")
    engine = PolarsQueryEngine(
        marts_dir=globals.project_root / "data/gold/marts",
        cache_ttl=settings.serving.query_cache_ttl_seconds,
    )
    app.state.engine = engine
    app.state.redis = None
    app.include_router(historic.router)
    app.include_router(realtime.router)
    app.include_router(health.router)
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.serving.app:app",
        host=settings.serving.host,
        port=settings.serving.port,
    )
