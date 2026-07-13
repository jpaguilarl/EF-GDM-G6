from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.serving.merged_view import MergedViewReader
from app.serving.query_engine import PolarsQueryEngine
from app.serving.routes.realtime import event_generator
from app.speed.pubsub import EventBus
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


def _write_mart(part_dir: Path, data: dict,
                part_values: dict[str, str | int],
                part_cols: list[str]) -> None:
    for col in part_cols:
        part_dir = part_dir / f"{col}={part_values[col]}"
    part_dir.mkdir(parents=True, exist_ok=True)
    data_cols = {k: v for k, v in data.items() if k not in part_cols}
    df = pl.DataFrame(data_cols)
    df.write_parquet(str(part_dir / "data.parquet"))


@pytest.fixture
def marts_dir(tmp_path: Path) -> Path:
    base = tmp_path / "marts"
    _write_mart(base / "mart_demand_volume", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "pickup_hour": [14], "bloque_horario": ["Mediodía"],
        "dia_semana": [3], "is_weekend": [False],
        "pu_location_id": [132], "pu_borough": ["Manhattan"],
        "pu_zone": ["Midtown"],
        "viajes": [47], "espera_total_min": [100.0],
        "viajes_con_espera": [40], "espera_promedio_min": [2.5],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1},
       ["service_id", "year", "month"])

    _write_mart(base / "mart_financial_performance", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "bloque_horario": ["Mediodía"], "pu_location_id": [132],
        "pu_borough": ["Manhattan"], "pu_zone": ["Midtown"],
        "viajes": [50], "total_amount": [2500.0],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1},
       ["service_id", "year", "month"])

    _write_mart(base / "mart_operational_profile", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "bloque_horario": ["Mediodía"], "pu_location_id": [132],
        "pu_borough": ["Manhattan"], "pu_zone": ["Midtown"],
        "viajes": [50], "duracion_total_min": [750.0],
        "duracion_promedio_min": [15.0],
        "distancia_total_millas": [250.0],
        "distancia_promedio_millas": [5.0],
        "velocidad_promedio_mph": [20.0],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1},
       ["service_id", "year", "month"])

    _write_mart(base / "mart_supply_demand_balance", {
        "location_id": [1], "borough": ["Manhattan"],
        "zone": ["Midtown"],
        "bloque_temporal_t": ["2025-01-15T14:00:00"],
        "bloque_temporal_t_plus_1": ["2025-01-15T14:15:00"],
        "taxis_entrantes_zona_t": [30],
        "taxis_salientes_zona_t_plus_1": [20],
        "flujo_neto_oferta": [10], "deficit_severo_flag": [False],
        "year": [2025], "month": [1],
    }, {"year": 2025, "month": 1}, ["year", "month"])

    _write_mart(base / "mart_abc_xyz_zones", {
        "pu_location_id": [1], "borough": ["Manhattan"],
        "zone": ["Midtown"], "service_id": ["yellow"], "year": [2025],
        "ingresos_totales_zona": [100000.0],
        "viajes_diarios_promedio": [500.0],
        "viajes_diarios_std": [50.0],
        "coeficiente_variacion_xyz": [0.1],
        "clase_xyz": ["X"],
        "porcentaje_acumulado_ingresos": [0.25],
        "clase_abc": ["A"],
    }, {"service_id": "yellow", "year": 2025},
       ["service_id", "year"])

    _write_mart(base / "mart_tipping_behavior", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "pu_borough": ["Manhattan"], "do_borough": ["Brooklyn"],
        "payment_type_id": [1], "is_credit_card": [True],
        "categoria_generosidad": ["Estandar"],
        "viajes": [30], "viajes_con_propina": [25],
        "propina_total": [150.0],
        "porcentaje_propina_promedio": [15.0],
        "porcentaje_propina_ponderado": [14.5],
        "propina_por_milla": [1.5],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1},
       ["service_id", "year", "month"])
    return base


@pytest.fixture
def app(marts_dir: Path):
    from fakeredis import FakeAsyncRedis

    from app.serving.routes import realtime as rt_routes

    app = FastAPI()
    engine = PolarsQueryEngine(marts_dir)
    redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
    redis_client._redis = FakeAsyncRedis(decode_responses=True)

    app.state.engine = engine
    app.state.redis = redis_client
    app.state.event_bus = EventBus()
    app.state.merged_reader = MergedViewReader(engine, redis_client)
    app.include_router(rt_routes.router)
    return app


class TestEventGenerator:
    async def test_snapshot_contains_batch_data(self):
        from fakeredis import FakeAsyncRedis

        engine = PolarsQueryEngine(Path("nonexistent"))
        redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        redis_client._redis = FakeAsyncRedis(decode_responses=True)
        bus = EventBus()
        reader = MergedViewReader(engine, redis_client)

        gen = event_generator(
            reader, bus, "mart_demand_volume", "pickup_hour", {},
        )
        event = await gen.__anext__()
        assert event["event"] == "snapshot"
        assert isinstance(event["data"], list)

    async def test_snapshot_with_filters(self, marts_dir):
        from fakeredis import FakeAsyncRedis

        engine = PolarsQueryEngine(marts_dir)
        redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        redis_client._redis = FakeAsyncRedis(decode_responses=True)
        bus = EventBus()
        reader = MergedViewReader(engine, redis_client)

        gen = event_generator(
            reader, bus, "mart_demand_volume", "pickup_hour",
            {"service_id": ["yellow"]},
        )
        event = await gen.__anext__()
        assert event["event"] == "snapshot"
        assert len(event["data"]) == 1
        assert event["data"][0]["viajes"] == 47

    async def test_increment_after_publish(self, marts_dir):
        from fakeredis import FakeAsyncRedis

        engine = PolarsQueryEngine(marts_dir)
        redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        redis_client._redis = FakeAsyncRedis(decode_responses=True)

        await redis_client.redis.hset(
            "rt:dv:yellow:2025-01-15:14:132", mapping={"viajes": "10"}
        )
        await redis_client.redis.expire("rt:dv:yellow:2025-01-15:14:132", 3600)

        bus = EventBus()
        reader = MergedViewReader(engine, redis_client)

        gen = event_generator(
            reader, bus, "mart_demand_volume", "pickup_hour",
            {"service_id": ["yellow"]},
        )
        event = await gen.__anext__()
        assert event["event"] == "snapshot"

        ride = EnrichedRide(
            trip_id=1,
            service_id="yellow",
            pickup_datetime=datetime(2025, 1, 15, 14, 0, 0),
            dropoff_datetime=None,
            pu_location_id=132,
            do_location_id=None,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Mediodía",
            franja_horaria="Tarde",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=14,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=10.0,
            fare_amount=10.0,
            tolls_amount=None,
        )
        await bus.publish(ride)

        event = await gen.__anext__()
        assert event["event"] == "increment"
        assert event["data"]["viajes"] == 10
        assert event["data"]["pu_location_id"] == 132

    async def test_generator_cleanup_unsubscribes(self, marts_dir):
        from fakeredis import FakeAsyncRedis

        engine = PolarsQueryEngine(marts_dir)
        redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        redis_client._redis = FakeAsyncRedis(decode_responses=True)
        bus = EventBus()
        reader = MergedViewReader(engine, redis_client)

        sub_count_before = len(bus._subscribers)

        gen = event_generator(
            reader, bus, "mart_demand_volume", "pickup_hour", {},
        )
        await gen.__anext__()  # snapshot
        assert len(bus._subscribers) == sub_count_before + 1

        await gen.aclose()
        assert len(bus._subscribers) == sub_count_before

    async def test_empty_mart_returns_empty_snapshot(self):
        from fakeredis import FakeAsyncRedis

        engine = PolarsQueryEngine(Path("nonexistent"))
        redis_client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        redis_client._redis = FakeAsyncRedis(decode_responses=True)
        bus = EventBus()
        reader = MergedViewReader(engine, redis_client)

        gen = event_generator(
            reader, bus, "mart_demand_volume", "pickup_hour", {},
        )
        event = await gen.__anext__()
        assert event["event"] == "snapshot"
        assert event["data"] == []


class TestGETEndpoints:
    VIEWS = [
        "demand-volume",
        "financial-performance",
        "operational-profile",
        "supply-demand",
        "tipping",
        "abc-xyz",
    ]

    @pytest.mark.parametrize("view", VIEWS)
    async def test_get_returns_200(self, view, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get(f"/api/v1/realtime/{view}")
            assert resp.status_code == 200

    @pytest.mark.parametrize("view", VIEWS)
    async def test_get_returns_json_array(self, view, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get(f"/api/v1/realtime/{view}")
            data = resp.json()
            assert isinstance(data, list)

    async def test_unknown_view_returns_404(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/v1/realtime/unknown")
            assert resp.status_code == 404
