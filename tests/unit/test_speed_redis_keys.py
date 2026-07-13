from datetime import datetime

import pytest

from app.schemas.settings_schema import SpeedConfig
from app.speed.aggregation import RealtimeAggregator
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


@pytest.fixture
def redis_client():
    from fakeredis import FakeAsyncRedis

    client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
    client._redis = FakeAsyncRedis(decode_responses=True)
    return client


class TestKeyNaming:
    async def test_demand_volume_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=1,
            service_id="green",
            pickup_datetime=datetime(2025, 7, 4, 14, 0, 0),
            dropoff_datetime=None,
            pu_location_id=100,
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
        await agg._update_demand_volume(ride)
        keys = await redis_client.redis.keys("rt:dv:*")
        assert len(keys) == 1
        assert keys[0] == "rt:dv:green:2025-07-04:14:100"

    async def test_financial_performance_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=2,
            service_id="yellow",
            pickup_datetime=datetime(2025, 7, 4, 8, 0, 0),
            dropoff_datetime=None,
            pu_location_id=200,
            do_location_id=None,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Punta Mañana",
            franja_horaria="Mañana",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=8,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=15.0,
            fare_amount=15.0,
            tolls_amount=None,
        )
        await agg._update_financial_performance(ride)
        keys = await redis_client.redis.keys("rt:fp:*")
        assert len(keys) == 1
        assert keys[0] == "rt:fp:yellow:2025-07-04:Punta Mañana:200"

    async def test_operational_profile_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=3,
            service_id="fhvhv",
            pickup_datetime=datetime(2025, 7, 4, 23, 0, 0),
            dropoff_datetime=None,
            pu_location_id=300,
            do_location_id=None,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Noche",
            franja_horaria="Noche",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=23,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=20.0,
            fare_amount=None,
            tolls_amount=None,
        )
        await agg._update_operational_profile(ride)
        keys = await redis_client.redis.keys("rt:op:*")
        assert len(keys) == 1
        assert keys[0] == "rt:op:fhvhv:2025-07-04:Noche:300"

    async def test_supply_demand_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=4,
            service_id="yellow",
            pickup_datetime=datetime(2025, 7, 4, 10, 30, 0),
            dropoff_datetime=datetime(2025, 7, 4, 10, 45, 0),
            pu_location_id=237,
            do_location_id=238,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Mediodía",
            franja_horaria="Mañana",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=10,
            trip_duration_minutes=15.0,
            passenger_group="Solo",
            revenue=15.0,
            fare_amount=12.0,
            tolls_amount=None,
        )
        await agg._update_supply_demand(ride)
        keys = await redis_client.redis.keys("rt:sd:*")
        assert len(keys) == 2
        ts = ride.pickup_datetime.timestamp()
        step = agg._block_seconds
        block = int(ts // step * step)
        assert f"rt:sd:237:{block}" in keys
        assert f"rt:sd:238:{block}" in keys

    async def test_tipping_behavior_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=5,
            service_id="yellow",
            pickup_datetime=datetime(2025, 7, 4, 10, 0, 0),
            dropoff_datetime=None,
            pu_location_id=237,
            do_location_id=238,
            pu_borough="Manhattan",
            pu_zone="Midtown",
            do_borough="Brooklyn",
            do_zone="Williamsburg",
            bloque_horario="Mediodía",
            franja_horaria="Mañana",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=10,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=20.0,
            fare_amount=15.0,
            tolls_amount=None,
            tip_amount=3.0,
            payment_type_id=1,
            categoria_generosidad="Estándar",
        )
        await agg._update_tipping_behavior(ride)
        keys = await redis_client.redis.keys("rt:tb:*")
        assert len(keys) == 1
        assert keys[0] == "rt:tb:yellow:2025-07-04:Manhattan:Brooklyn:1:Estándar"

    async def test_tipping_behavior_null_borough_default(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=6,
            service_id="yellow",
            pickup_datetime=datetime(2025, 7, 4, 10, 0, 0),
            dropoff_datetime=None,
            pu_location_id=None,
            do_location_id=None,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Mediodía",
            franja_horaria="Mañana",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=10,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=10.0,
            fare_amount=10.0,
            tolls_amount=None,
            tip_amount=None,
            payment_type_id=None,
            categoria_generosidad=None,
        )
        await agg._update_tipping_behavior(ride)
        keys = await redis_client.redis.keys("rt:tb:*")
        assert len(keys) == 1
        assert keys[0] == "rt:tb:yellow:2025-07-04:Desconocido:Desconocido:0:Sin Propina"

    async def test_uniqueness_key(self, redis_client):
        config = SpeedConfig()
        agg = RealtimeAggregator(redis_client, config)
        ride = EnrichedRide(
            trip_id=999,
            service_id="yellow",
            pickup_datetime=datetime(2025, 7, 4, 10, 0, 0),
            dropoff_datetime=None,
            pu_location_id=None,
            do_location_id=None,
            pu_borough=None,
            pu_zone=None,
            do_borough=None,
            do_zone=None,
            bloque_horario="Mediodía",
            franja_horaria="Mañana",
            dia_categoria="Día Laborable",
            is_weekend=False,
            pickup_hour=10,
            trip_duration_minutes=None,
            passenger_group="Solo",
            revenue=10.0,
            fare_amount=10.0,
            tolls_amount=None,
        )
        await agg._update_uniqueness(ride)
        seen_key = "rt:seen:999"
        exists = await redis_client.redis.get(seen_key)
        assert exists == "1"


class TestTTL:
    async def test_default_ttl_is_48h(self):
        client = RedisClient("redis://localhost:6379/0")
        assert client._ttl == 48 * 3600

    async def test_custom_ttl(self):
        client = RedisClient("redis://localhost:6379/0", ttl_hours=24)
        assert client._ttl == 24 * 3600

    async def test_connect_and_close(self):
        from fakeredis import FakeAsyncRedis

        client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        client._redis = FakeAsyncRedis(decode_responses=True)
        await client.close()
        assert client._redis is not None

    async def test_redis_property_raises_if_not_connected(self):
        client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
        with pytest.raises(AssertionError, match="Redis not connected"):
            _ = client.redis
