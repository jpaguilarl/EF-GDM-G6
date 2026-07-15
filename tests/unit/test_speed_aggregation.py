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


@pytest.fixture
def config() -> SpeedConfig:
    return SpeedConfig()


@pytest.fixture
def aggregator(redis_client, config) -> RealtimeAggregator:
    return RealtimeAggregator(redis_client, config)


@pytest.fixture
def ride() -> EnrichedRide:
    return EnrichedRide(
        trip_id=12345,
        service_id="yellow",
        pickup_datetime=datetime(2025, 6, 15, 10, 30, 0),
        dropoff_datetime=datetime(2025, 6, 15, 10, 45, 0),
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
        trip_duration_minutes=15.0,
        passenger_group="Pareja",
        revenue=20.0,
        fare_amount=15.0,
        tolls_amount=2.5,
        tip_amount=3.0,
        payment_type_id=1,
        trip_distance=5.0,
        extra=0.5,
        mta_tax=0.5,
        total_amount=20.0,
        base_passenger_fare=None,
        tips=None,
        driver_pay=None,
        trip_miles=None,
        shared_request_flag=None,
        shared_match_flag=None,
        categoria_generosidad="Estándar",
    )


class TestUniqueness:
    async def test_new_event_returns_true(self, aggregator, ride):
        result = await aggregator._update_uniqueness(ride)
        assert result is True

    async def test_duplicate_event_returns_false(self, aggregator, ride):
        await aggregator._update_uniqueness(ride)
        result = await aggregator._update_uniqueness(ride)
        assert result is False

    async def test_duplicate_skips_aggregation(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        await aggregator.on_event(ride)
        dv_key = f"rt:dv:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pickup_hour}:{ride.pu_location_id}"
        viajes = await redis_client.redis.hget(dv_key, "viajes")
        assert viajes == "1"


class TestDemandVolume:
    async def test_increments_viajes(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        dv_key = f"rt:dv:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pickup_hour}:{ride.pu_location_id}"
        viajes = await redis_client.redis.hget(dv_key, "viajes")
        assert viajes == "1"

    async def test_sets_expiry(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        dv_key = f"rt:dv:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pickup_hour}:{ride.pu_location_id}"
        ttl = await redis_client.redis.ttl(dv_key)
        assert 0 < ttl <= 48 * 3600

    async def test_increments_accumulates(self, aggregator, redis_client):
        r1 = EnrichedRide(
            trip_id=1,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 30, 0),
            dropoff_datetime=datetime(2025, 6, 15, 10, 45, 0),
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
            trip_duration_minutes=15.0,
            passenger_group="Pareja",
            revenue=20.0,
            fare_amount=15.0,
            tolls_amount=2.5,
            tip_amount=3.0,
            payment_type_id=1,
            trip_distance=5.0,
            extra=0.5,
            mta_tax=0.5,
            total_amount=20.0,
            categoria_generosidad="Estándar",
        )
        r2 = EnrichedRide(
            trip_id=2,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 30, 0),
            dropoff_datetime=datetime(2025, 6, 15, 10, 50, 0),
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
            trip_duration_minutes=20.0,
            passenger_group="Solo",
            revenue=25.0,
            fare_amount=20.0,
            tolls_amount=3.0,
            tip_amount=4.0,
            payment_type_id=1,
            trip_distance=7.0,
            extra=1.0,
            mta_tax=0.5,
            total_amount=25.0,
            categoria_generosidad="Estándar",
        )
        await aggregator.on_event(r1)
        await aggregator.on_event(r2)
        dv_key = "rt:dv:yellow:2025-06-15:10:237"
        viajes = await redis_client.redis.hget(dv_key, "viajes")
        assert viajes == "2"


class TestFinancialPerformance:
    async def test_yellow_fare_components(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        fp_key = f"rt:fp:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"
        viajes = await redis_client.redis.hget(fp_key, "viajes")
        assert viajes == "1"
        fare = await redis_client.redis.hget(fp_key, "fare_amount")
        assert float(fare) == pytest.approx(15.0)
        tip = await redis_client.redis.hget(fp_key, "tip_amount")
        assert float(tip) == pytest.approx(3.0)

    async def test_fhvhv_fare_components(self, aggregator, redis_client):
        ride = EnrichedRide(
            trip_id=99,
            service_id="fhvhv",
            pickup_datetime=datetime(2025, 6, 15, 10, 30, 0),
            dropoff_datetime=datetime(2025, 6, 15, 10, 45, 0),
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
            trip_duration_minutes=15.0,
            passenger_group="Solo",
            revenue=25.0,
            fare_amount=None,
            tolls_amount=5.0,
            base_passenger_fare=25.0,
            tips=3.0,
            driver_pay=20.0,
            categoria_generosidad=None,
        )
        await aggregator.on_event(ride)
        fp_key = "rt:fp:fhvhv:2025-06-15:Mediodía:237"
        viajes = await redis_client.redis.hget(fp_key, "viajes")
        assert viajes == "1"
        bpf = await redis_client.redis.hget(fp_key, "base_passenger_fare")
        assert float(bpf) == pytest.approx(25.0)
        tolls = await redis_client.redis.hget(fp_key, "tolls")
        assert float(tolls) == pytest.approx(5.0)
        tips = await redis_client.redis.hget(fp_key, "tips")
        assert float(tips) == pytest.approx(3.0)
        driver_pay = await redis_client.redis.hget(fp_key, "driver_pay")
        assert float(driver_pay) == pytest.approx(20.0)


class TestOperationalProfile:
    async def test_stores_duration_and_distance(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        op_key = f"rt:op:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"
        viajes = await redis_client.redis.hget(op_key, "viajes")
        assert viajes == "1"
        duracion = await redis_client.redis.hget(op_key, "duracion_total_min")
        assert float(duracion) == pytest.approx(15.0)
        distancia = await redis_client.redis.hget(op_key, "distancia_total_millas")
        assert float(distancia) == pytest.approx(5.0)


class TestSupplyDemand:
    async def test_pickup_creates_salientes(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        ts = ride.pickup_datetime.timestamp()
        step = aggregator._block_seconds
        block_unix = int(ts // step * step)
        sd_key = f"rt:sd:{ride.pu_location_id}:{block_unix}"
        salientes = await redis_client.redis.hget(sd_key, "salientes")
        assert salientes == "1"

    async def test_dropoff_creates_entrantes(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        ts = ride.pickup_datetime.timestamp()
        step = aggregator._block_seconds
        block_unix = int(ts // step * step)
        sd_key = f"rt:sd:{ride.do_location_id}:{block_unix}"
        entrantes = await redis_client.redis.hget(sd_key, "entrantes")
        assert entrantes == "1"


class TestTippingBehavior:
    async def test_stores_tip_aggregates(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        tb_key = f"rt:tb:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pu_borough}:{ride.do_borough}:{ride.payment_type_id}:{ride.categoria_generosidad}"
        viajes = await redis_client.redis.hget(tb_key, "viajes")
        assert viajes == "1"
        propina = await redis_client.redis.hget(tb_key, "propina_total")
        assert float(propina) == pytest.approx(3.0)
        base = await redis_client.redis.hget(tb_key, "tarifa_base_sum")
        assert float(base) == pytest.approx(15.0)


class TestRedisKeys:
    async def test_demand_volume_key_format(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        keys = await redis_client.redis.keys("rt:dv:*")
        assert len(keys) == 1
        assert keys[0].startswith("rt:dv:yellow:2025-06-15:10:237")

    async def test_keys_have_ttl(self, aggregator, ride, redis_client):
        await aggregator.on_event(ride)
        all_keys = await redis_client.redis.keys("rt:*")
        for key in all_keys:
            ttl = await redis_client.redis.ttl(key)
            assert 0 < ttl <= 48 * 3600, f"Key {key} has no TTL"
