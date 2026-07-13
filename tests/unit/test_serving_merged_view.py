from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from app.serving.merged_view import MergedViewReader
from app.serving.query_engine import PolarsQueryEngine
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


@pytest.fixture
def redis_client():
    from fakeredis import FakeAsyncRedis

    client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
    client._redis = FakeAsyncRedis(decode_responses=True)
    return client


@pytest.fixture
def marts_dir(tmp_path: Path) -> Path:
    base = tmp_path / "marts"
    mart_dir = base / "mart_demand_volume"
    part = mart_dir / "service_id=yellow" / "year=2025" / "month=1"
    part.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "service_id": ["yellow"],
        "fecha_viaje": ["2025-01-15"],
        "pickup_hour": [14],
        "bloque_horario": ["Mediodía"],
        "dia_semana": [3],
        "is_weekend": [False],
        "pu_location_id": [132],
        "pu_borough": ["Manhattan"],
        "pu_zone": ["Midtown"],
        "viajes": [47],
        "espera_total_min": [100.0],
        "viajes_con_espera": [40],
        "espera_promedio_min": [2.5],
        "year": [2025],
        "month": [1],
    })
    df.write_parquet(str(part / "data.parquet"))
    return base


@pytest.fixture
def engine(marts_dir: Path) -> PolarsQueryEngine:
    return PolarsQueryEngine(marts_dir)


class TestMergedViewReader:
    async def test_returns_batch_data_when_no_redis(self, engine, redis_client):
        reader = MergedViewReader(engine, redis_client)
        result = await reader.read_merged(
            "mart_demand_volume",
            "pickup_hour",
            filter_cols={"service_id": ["yellow"]},
        )
        assert len(result) == 1
        assert result[0]["viajes"] == 47
        assert result[0]["service_id"] == "yellow"

    async def test_batch_wins_on_overlap(self, engine, redis_client):
        await redis_client.redis.hset(
            "rt:dv:yellow:2025-01-15:14:132", mapping={"viajes": "10"}
        )
        await redis_client.redis.expire("rt:dv:yellow:2025-01-15:14:132", 3600)

        reader = MergedViewReader(engine, redis_client)
        result = await reader.read_merged(
            "mart_demand_volume",
            "pickup_hour",
            filter_cols={"service_id": ["yellow"]},
        )
        assert result[0]["viajes"] == 47

    async def test_adds_unique_redis_rows(self, engine, redis_client):
        await redis_client.redis.hset(
            "rt:dv:yellow:2025-01-15:15:132", mapping={"viajes": "5"}
        )
        await redis_client.redis.expire("rt:dv:yellow:2025-01-15:15:132", 3600)

        reader = MergedViewReader(engine, redis_client)
        result = await reader.read_merged(
            "mart_demand_volume",
            "pickup_hour",
            filter_cols={"service_id": ["yellow"]},
        )
        assert len(result) == 2
        hours = {r["pickup_hour"] for r in result}
        assert hours == {14, 15}

    async def test_redis_rows_filtered_correctly(self, engine, redis_client):
        await redis_client.redis.hset(
            "rt:dv:green:2025-01-15:14:200", mapping={"viajes": "8"}
        )
        await redis_client.redis.expire("rt:dv:green:2025-01-15:14:200", 3600)

        reader = MergedViewReader(engine, redis_client)
        result = await reader.read_merged(
            "mart_demand_volume",
            "pickup_hour",
            filter_cols={"service_id": ["yellow"]},
        )
        assert all(r["service_id"] == "yellow" for r in result)

    async def test_get_realtime_row_returns_updated(self, redis_client):
        await redis_client.redis.hset(
            "rt:dv:yellow:2025-01-15:14:132", mapping={"viajes": "10"}
        )
        await redis_client.redis.expire("rt:dv:yellow:2025-01-15:14:132", 3600)

        reader = MergedViewReader(None, redis_client)  # type: ignore[arg-type]
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
        row = await reader.get_realtime_row("mart_demand_volume", ride)
        assert row is not None
        assert row["viajes"] == 10
        assert row["pu_location_id"] == 132

    async def test_get_realtime_row_none_for_unknown(self, engine, redis_client):
        reader = MergedViewReader(engine, redis_client)
        ride = EnrichedRide(
            trip_id=1,
            service_id="yellow",
            pickup_datetime=datetime(2025, 1, 15, 14, 0, 0),
            dropoff_datetime=None,
            pu_location_id=999,
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
        row = await reader.get_realtime_row("mart_demand_volume", ride)
        assert row is None

    async def test_abc_xyz_from_redis(self, redis_client):
        await redis_client.redis.hset(
            "rt:dv:yellow:2025-01-15:14:132", mapping={"viajes": "5"}
        )
        await redis_client.redis.expire("rt:dv:yellow:2025-01-15:14:132", 3600)
        await redis_client.redis.hset(
            "rt:fp:yellow:2025-01-15:Mediodía:132", mapping={"total_amount": "100.0"}
        )
        await redis_client.redis.expire("rt:fp:yellow:2025-01-15:Mediodía:132", 3600)

        reader = MergedViewReader(None, redis_client)  # type: ignore[arg-type]
        rows = await reader._get_abc_xyz_redis_rows({})
        assert len(rows) == 1
        assert rows[0]["pu_location_id"] == 132
        assert rows[0]["viajes_realtime"] == 5
        assert rows[0]["ingresos_realtime"] == 100.0

    async def test_empty_redis_returns_empty(self, engine, redis_client):
        reader = MergedViewReader(engine, redis_client)
        rows = await reader._get_redis_rows("mart_demand_volume", {})
        assert rows == []

    async def test_realtime_row_none_for_abc_xyz(self, engine, redis_client):
        reader = MergedViewReader(engine, redis_client)
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
        row = await reader.get_realtime_row("mart_abc_xyz_zones", ride)
        assert row is None

    async def test_supply_demand_key_parse(self, redis_client):
        block_ts = int(datetime(2025, 1, 15, 14, 0, 0).timestamp())
        block_key = block_ts // 900 * 900
        await redis_client.redis.hset(
            f"rt:sd:132:{block_key}", mapping={"entrantes": "15", "salientes": "10"}
        )
        await redis_client.redis.expire(f"rt:sd:132:{block_key}", 3600)

        reader = MergedViewReader(None, redis_client)  # type: ignore[arg-type]
        rows = await reader._get_redis_rows("mart_supply_demand_balance", {})
        assert len(rows) == 1
        assert rows[0]["location_id"] == 132
        assert rows[0]["entrantes"] == 15
        assert rows[0]["salientes"] == 10
        assert rows[0]["flujo_neto_oferta"] == -5
        assert rows[0]["deficit_severo_flag"] is False
