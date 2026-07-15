from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import xxhash

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.serving.app import create_app
from app.serving.merged_view import MergedViewReader
from app.serving.query_engine import PolarsQueryEngine
from app.speed.aggregation import RealtimeAggregator
from app.speed.event_processor import EventProcessor
from app.speed.ingest import router as ingest_router
from app.speed.pubsub import EventBus
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


@pytest.mark.integration
class TestServingE2E:
    @pytest.fixture(autouse=True)
    async def setup_redis(self):
        from fakeredis import FakeAsyncRedis
        self.redis = FakeAsyncRedis(decode_responses=True)

    @pytest.fixture
    def redis_client(self):
        class SyncRedis:
            def __init__(self, r):
                self._r = r

            @property
            def redis(self):
                return self._r

            @property
            def _ttl(self):
                return 172800

        return SyncRedis(self.redis)

    @pytest.fixture
    def bus(self):
        return EventBus()

    @pytest.fixture
    def aggregator(self, redis_client):
        from app.schemas.settings_schema import SpeedConfig
        return RealtimeAggregator(redis_client, SpeedConfig())

    @pytest.fixture
    def client(self, bus, aggregator, redis_client):
        bus.subscribe(aggregator.on_event)

        app = FastAPI()

        @app.on_event("startup")
        async def startup():
            app.state.redis = redis_client
            app.state.bus = bus

        app.state.event_bus = bus
        app.state.redis = redis_client

        from app.serving.routes import health, realtime
        app.include_router(health.router)
        app.include_router(realtime.router)
        app.include_router(ingest_router)

        app.state.processor = self._mock_processor()
        app.state.merged_reader = MergedViewReader(
            engine=self._mock_engine(),
            redis=redis_client,
            block_minutes=15,
        )

        with TestClient(app) as c:
            yield c

    def _mock_processor(self):
        p = MagicMock()

        def process_side_effect(event):
            # reject when required field is missing (mirrors EventProcessor._check_completeness)
            if event.service_id == "yellow" and event.vendor_id is None:
                return None
            trip_id = xxhash.xxh64(
                f"{event.vendor_id}|{event.pickup_datetime}|{event.dropoff_datetime}|{event.pu_location_id}",
                seed=0,
            ).intdigest()
            return EnrichedRide(
                trip_id=trip_id,
                service_id=event.service_id or "yellow",
                pickup_datetime=event.pickup_datetime or datetime.now(timezone.utc),
                dropoff_datetime=event.dropoff_datetime,
                pu_location_id=event.pu_location_id or 237,
                do_location_id=event.do_location_id or 238,
                pu_borough="Manhattan",
                pu_zone="Midtown",
                do_borough="Brooklyn",
                do_zone="Williamsburg",
                bloque_horario="Mediodía",
                franja_horaria="Tarde",
                dia_categoria="Día Laborable",
                is_weekend=False,
                pickup_hour=14,
                trip_duration_minutes=15.0,
                passenger_group="Solo",
                revenue=20.0,
                fare_amount=15.0,
                tolls_amount=2.5,
                tip_amount=3.0,
                total_amount=23.0,
                payment_type_id=1,
                trip_distance=3.5,
                categoria_generosidad="Estandar",
                shared_request_flag=None,
                shared_match_flag=None,
                base_passenger_fare=None,
                trip_miles=None,
                extra=0.5,
                mta_tax=0.5,
                tips=None,
                driver_pay=None,
            )

        p.process = process_side_effect
        return p

    def _mock_engine(self):
        import polars as pl
        engine = MagicMock(spec=PolarsQueryEngine)

        def query_side_effect(mart, filters=None, **kwargs):
            return pl.DataFrame()

        engine.query = query_side_effect
        return engine

    @pytest.mark.asyncio
    async def test_ingest_creates_redis_keys(self, client, redis_client, aggregator):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-07-12T14:30:00",
            "dropoff_datetime": "2025-07-12T14:45:00",
            "vendor_id": 1,
            "pu_location_id": 237,
            "do_location_id": 238,
            "passenger_count": 1,
            "fare_amount": 15.0,
            "total_amount": 23.0,
        }
        resp = client.post("/api/v1/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"

        await asyncio.sleep(0.1)

        keys = await self.redis.keys("rt:*")
        assert len(keys) > 0

        dv_keys = [k for k in keys if k.startswith("rt:dv:")]
        assert len(dv_keys) >= 1
        stats = await self.redis.hgetall(dv_keys[0])
        assert int(stats.get("viajes", 0)) >= 1

    @pytest.mark.asyncio
    async def test_ingest_then_merged_get(self, client, redis_client, aggregator):
        payloads = [
            {
                "service_id": "yellow",
                "pickup_datetime": "2025-07-12T14:30:00",
                "dropoff_datetime": "2025-07-12T14:45:00",
                "vendor_id": 1,
                "pu_location_id": 100,
                "do_location_id": 200,
                "fare_amount": 20.0,
                "total_amount": 28.0,
                "tip_amount": 4.0,
            },
            {
                "service_id": "yellow",
                "pickup_datetime": "2025-07-12T14:35:00",
                "dropoff_datetime": "2025-07-12T14:50:00",
                "vendor_id": 2,
                "pu_location_id": 100,
                "do_location_id": 200,
                "fare_amount": 18.0,
                "total_amount": 25.0,
                "tip_amount": 3.0,
            },
        ]

        for p in payloads:
            resp = client.post("/api/v1/ingest", json=p)
            assert resp.status_code == 200

        await asyncio.sleep(0.1)

        resp = client.get("/api/v1/realtime/demand-volume?service_id=yellow&pu_location_id=100")
        assert resp.status_code == 200
        merged = resp.json()
        assert len(merged) >= 1
        row = merged[0]
        assert row["service_id"] == "yellow"
        assert row["pu_location_id"] == 100
        assert row["viajes"] >= 2

    def test_ingest_rejected_event(self, client):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-07-12T14:30:00",
            "vendor_id": None,
        }
        resp = client.post("/api/v1/ingest", json=payload)
        assert resp.status_code == 422
        assert resp.json()["status"] == "rejected"

    def test_health_endpoint(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    @pytest.mark.asyncio
    async def test_supply_demand_redis_key(self, client, redis_client, aggregator):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-07-12T15:00:00",
            "dropoff_datetime": "2025-07-12T15:15:00",
            "vendor_id": 1,
            "pu_location_id": 50,
            "do_location_id": 100,
            "fare_amount": 12.0,
            "total_amount": 16.0,
        }
        client.post("/api/v1/ingest", json=payload)
        await asyncio.sleep(0.1)

        sd_keys = await self.redis.keys("rt:sd:*")
        assert len(sd_keys) >= 2

        pu_key = [k for k in sd_keys if str(k).split(":")[2] == "50"]
        do_key = [k for k in sd_keys if str(k).split(":")[2] == "100"]

        if pu_key:
            sd = await self.redis.hgetall(pu_key[0])
            assert int(sd.get("salientes", 0)) >= 1

        if do_key:
            sd = await self.redis.hgetall(do_key[0])
            assert int(sd.get("entrantes", 0)) >= 1

    @pytest.mark.asyncio
    async def test_deduplication_same_trip_id(self, client, redis_client, aggregator):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-07-12T16:00:00",
            "dropoff_datetime": "2025-07-12T16:10:00",
            "vendor_id": 1,
            "pu_location_id": 10,
            "do_location_id": 20,
            "fare_amount": 10.0,
            "total_amount": 14.0,
        }

        client.post("/api/v1/ingest", json=payload)
        await asyncio.sleep(0.05)
        client.post("/api/v1/ingest", json=payload)
        await asyncio.sleep(0.05)

        dv_keys = [k for k in await self.redis.keys("rt:dv:*") if "10" in k.split(":")]
        if dv_keys:
            stats = await self.redis.hgetall(dv_keys[0])
            assert int(stats.get("viajes", 0)) == 1
