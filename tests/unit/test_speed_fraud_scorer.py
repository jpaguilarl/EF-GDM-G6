from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest
from sklearn.ensemble import IsolationForest

from app.schemas.settings_schema import SpeedConfig
from app.speed.fraud_scorer import FraudScorer
from app.speed.schema import EnrichedRide


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
    X = np.random.default_rng(42).normal(size=(50, 6))
    model = IsolationForest(n_estimators=10, contamination=0.1, random_state=42, n_jobs=1)
    model.fit(X)
    loader.if_models = {1: model, 2: model}
    loader.kmodes_models = {}
    loader.kmodes_mappings = {}
    loader.flat_fares = {2: {2025: 70.0}}
    return loader


@pytest.fixture
def config():
    return SpeedConfig(fraud_score_threshold=-0.1)


@pytest.fixture
def scorer(model_loader, config, redis_client):
    return FraudScorer(model_loader, config, redis_client)


class TestFeatureComputation:
    def test_compute_features_basic(self, scorer):
        ride = EnrichedRide(
            trip_id=1,
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
        )
        features = scorer._compute_features(ride)
        assert features is not None
        assert features["trip_distance"] == 10.0
        assert features["fare_amount"] == 25.0
        assert features["duracion_viaje_segundos"] == 1800.0
        assert features["velocidad_promedio_calculada"] == pytest.approx(20.0, rel=0.1)
        assert features["costo_por_distancia"] == pytest.approx(2.5, rel=0.01)
        assert features["ratio_peaje_tarifa"] == pytest.approx(0.2, rel=0.01)

    def test_compute_features_missing_distance(self, scorer):
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
            fare_amount=25.0,
            tolls_amount=None,
            trip_distance=None,
        )
        assert scorer._compute_features(ride) is None

    def test_compute_features_missing_fare(self, scorer):
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
            fare_amount=None,
            tolls_amount=None,
            trip_distance=5.0,
        )
        assert scorer._compute_features(ride) is None

    def test_compute_features_no_dropoff(self, scorer):
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
            tolls_amount=None,
            trip_distance=3.0,
        )
        features = scorer._compute_features(ride)
        assert features is not None
        assert features["duracion_viaje_segundos"] is None
        assert features["velocidad_promedio_calculada"] is None


class TestFlatFareFor:
    def test_flat_fare_jfk(self, scorer):
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
            revenue=70.0,
            fare_amount=70.0,
            ratecode_id=2,
            tolls_amount=None,
            trip_distance=None,
        )
        assert scorer._flat_fare_for(ride) == 70.0

    def test_flat_fare_no_ratecode(self, scorer):
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
            ratecode_id=None,
            tolls_amount=None,
            trip_distance=None,
        )
        assert scorer._flat_fare_for(ride) is None


class TestOnEvent:
    async def test_on_event_stores_score(self, scorer, redis_client):
        ride = EnrichedRide(
            trip_id=1001,
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
            ratecode_id=1,
        )
        await scorer.on_event(ride)

        stored = await redis_client.redis.hgetall("rt:fraud:1001")
        assert stored is not None
        assert stored["trip_id"] == "1001"
        assert stored["service_id"] == "yellow"
        assert stored["ratecode_id"] == "1"

    async def test_on_event_no_model(self, scorer, redis_client):
        ride = EnrichedRide(
            trip_id=2002,
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
            ratecode_id=99,
        )
        await scorer.on_event(ride)

        stored = await redis_client.redis.hgetall("rt:fraud:2002")
        assert stored is not None
        assert stored["anomaly_score"] == ""
        assert stored["is_fraud"] == "False"

    async def test_on_event_skips_missing_features(self, scorer, redis_client):
        ride = EnrichedRide(
            trip_id=3003,
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
            fare_amount=None,
            tolls_amount=None,
            trip_distance=None,
        )
        await scorer.on_event(ride)
        exists = await redis_client.redis.exists("rt:fraud:3003")
        assert not exists
