from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest
from kmodes.kmodes import KModes

from app.speed.schema import EnrichedRide
from app.speed.trip_profiler import TripProfiler


@pytest.fixture
def redis_client():
    from fakeredis import FakeAsyncRedis

    from app.speed.redis_client import RedisClient

    client = RedisClient("redis://localhost:6379/0", ttl_hours=48)
    client._redis = FakeAsyncRedis(decode_responses=True)
    return client


@pytest.fixture
def model_loader():
    loader = MagicMock()

    X = np.array(
        [[0, 0, 0, 0, 0, 0, 0], [1, 1, 1, 1, 1, 1, 1], [0, 0, 0, 0, 0, 0, 0]],
        dtype=np.int32,
    )
    km = KModes(n_clusters=2, init="Cao", n_init=1, random_state=42)
    km.fit(X)

    loader.kmodes_models = {"yellow": km, "green": km}
    loader.kmodes_mappings = {
        "yellow": {
            "borough_pu": {"0": "Manhattan", "1": "Brooklyn"},
            "borough_do": {"0": "Queens", "1": "Bronx"},
            "franja_horaria": {"0": "Mañana", "1": "Tarde"},
            "dia_categoria": {"0": "Día Laborable", "1": "Fin de Semana"},
            "payment_type": {"0": "1", "1": "2"},
            "ratecode": {"0": "1", "1": "2"},
            "passenger_group": {"0": "Solo", "1": "Pareja"},
        }
    }
    loader.if_models = {}
    loader.flat_fares = {}
    return loader


@pytest.fixture
def profiler(model_loader, redis_client):
    return TripProfiler(model_loader, redis_client)


class TestExtractFeatures:
    def test_extract_yellow_features(self, profiler):
        ride = EnrichedRide(
            trip_id=1,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
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
            revenue=None,
            fare_amount=15.0,
            payment_type_id=1,
            ratecode_id=1,
            tolls_amount=None,
            trip_distance=None,
        )
        features = profiler._extract_features(ride)
        assert features is not None
        assert features["borough_pu"] == "Manhattan"
        assert features["borough_do"] == "Brooklyn"
        assert features["franja_horaria"] == "Mañana"
        assert features["dia_categoria"] == "Día Laborable"
        assert features["payment_type"] == "1"
        assert features["ratecode"] == "1"
        assert features["passenger_group"] == "Solo"

    def test_extract_fhvhv_features(self, profiler):
        ride = EnrichedRide(
            trip_id=1,
            service_id="fhvhv",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
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
            revenue=None,
            fare_amount=25.0,
            hvfhs_license_num="HV0001",
            tolls_amount=None,
            trip_distance=None,
        )
        features = profiler._extract_features(ride)
        assert features is not None
        assert features["borough_pu"] == "Manhattan"
        assert features["hvfhs_license_num"] == "HV0001"

    def test_extract_features_missing_returns_none(self, profiler):
        ride = EnrichedRide(
            trip_id=1,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
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
            revenue=None,
            fare_amount=15.0,
            payment_type_id=None,
            ratecode_id=None,
            tolls_amount=None,
            trip_distance=None,
        )
        assert profiler._extract_features(ride) is None


class TestEncode:
    def test_known_value(self, profiler):
        mapping = {
            "borough_pu": {"0": "Manhattan", "1": "Brooklyn"},
        }
        code = profiler._encode(mapping, "borough_pu", "Manhattan")
        assert code == 0

    def test_unknown_value(self, profiler):
        mapping = {
            "borough_pu": {"0": "Manhattan", "1": "Brooklyn"},
        }
        code = profiler._encode(mapping, "borough_pu", "Staten Island")
        assert code == -1

    def test_unknown_column(self, profiler):
        mapping = {"borough_pu": {"0": "Manhattan"}}
        code = profiler._encode(mapping, "nonexistent", "foo")
        assert code == -1


class TestOnEvent:
    async def test_on_event_stores_cluster(self, profiler, redis_client):
        ride = EnrichedRide(
            trip_id=101,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
            dropoff_datetime=datetime(2025, 6, 15, 10, 30, 0),
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
            trip_duration_minutes=30.0,
            passenger_group="Solo",
            revenue=25.0,
            fare_amount=25.0,
            tolls_amount=5.0,
            trip_distance=10.0,
            payment_type_id=1,
            ratecode_id=1,
        )
        await profiler.on_event(ride)

        stored = await redis_client.redis.hgetall("rt:cluster:101")
        assert stored is not None
        assert "cluster_id" in stored
        assert stored["service_id"] == "yellow"

    async def test_on_event_no_model(self, profiler, redis_client):
        ride = EnrichedRide(
            trip_id=202,
            service_id="fhvhv",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
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
            revenue=None,
            fare_amount=25.0,
            hvfhs_license_num="HV0001",
            tolls_amount=None,
            trip_distance=None,
        )
        await profiler.on_event(ride)
        exists = await redis_client.redis.exists("rt:cluster:202")
        assert not exists

    async def test_on_event_missing_features(self, profiler, redis_client):
        ride = EnrichedRide(
            trip_id=303,
            service_id="yellow",
            pickup_datetime=datetime(2025, 6, 15, 10, 0, 0),
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
            passenger_group="Desconocido",
            revenue=None,
            fare_amount=None,
            payment_type_id=None,
            ratecode_id=None,
            tolls_amount=None,
            trip_distance=None,
        )
        await profiler.on_event(ride)
        exists = await redis_client.redis.exists("rt:cluster:303")
        assert not exists
