from datetime import datetime

import numpy as np

from app.pipeline.gold_impl.feature_rules import ratecode_tariff as rt
from app.schemas.settings_schema import SpeedConfig
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


class FraudScorer:
    FEATURE_COLS = [
        "velocidad_promedio_calculada",
        "costo_por_distancia",
        "duracion_viaje_segundos",
        "trip_distance",
        "fare_amount",
        "ratio_peaje_tarifa",
    ]

    def __init__(self, model_loader, config: SpeedConfig, redis: RedisClient):
        self.models = model_loader
        self.config = config
        self.redis = redis
        self._flat_fares = model_loader.flat_fares

    async def on_event(self, ride: EnrichedRide) -> None:
        features = self._compute_features(ride)
        if features is None:
            return

        flat_fare = self._flat_fare_for(ride)
        is_candidate = rt.is_anomaly_candidate_py(
            ratecode=ride.ratecode_id,
            fare=ride.fare_amount,
            flat_fare=flat_fare,
            speed_mph=features["velocidad_promedio_calculada"],
            cost_per_mile=features["costo_por_distancia"],
        )

        model = self.models.if_models.get(ride.ratecode_id)
        anomaly_score = None
        is_fraud = False
        if model is not None:
            X = np.array([[features[c] for c in self.FEATURE_COLS]])
            X = np.nan_to_num(X, nan=0.0)
            anomaly_score = float(-model.decision_function(X)[0])
            is_fraud = anomaly_score > self.config.fraud_score_threshold

        await self._store_score(
            ride.trip_id,
            {
                "trip_id": ride.trip_id,
                "service_id": ride.service_id,
                "ratecode_id": ride.ratecode_id,
                "anomaly_score": anomaly_score,
                "is_fraud": is_fraud,
                "is_anomaly_candidate": is_candidate,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def _compute_features(self, ride: EnrichedRide) -> dict | None:
        if ride.trip_distance is None or ride.fare_amount is None:
            return None

        dur_seg = None
        if ride.dropoff_datetime and ride.dropoff_datetime > ride.pickup_datetime:
            dur_seg = (ride.dropoff_datetime - ride.pickup_datetime).total_seconds()

        velocidad = None
        if dur_seg and dur_seg > 0 and ride.trip_distance > 0:
            velocidad = round(ride.trip_distance / (dur_seg / 3600.0), 2)

        costo = round(ride.fare_amount / (ride.trip_distance + 0.001), 4)

        ratio_peaje = None
        if ride.tolls_amount is not None and ride.fare_amount > 0:
            ratio_peaje = round(ride.tolls_amount / ride.fare_amount, 4)

        return {
            "velocidad_promedio_calculada": velocidad,
            "costo_por_distancia": costo,
            "duracion_viaje_segundos": dur_seg,
            "trip_distance": ride.trip_distance,
            "fare_amount": ride.fare_amount,
            "ratio_peaje_tarifa": ratio_peaje,
        }

    def _flat_fare_for(self, ride: EnrichedRide) -> float | None:
        year = ride.pickup_datetime.year
        fares_by_year = (
            self._flat_fares.get(ride.ratecode_id) if ride.ratecode_id else None
        )
        if fares_by_year is None:
            return None
        return fares_by_year.get(year)

    async def _store_score(self, trip_id: int, data: dict) -> None:
        pipe = self.redis.redis.pipeline()
        serialized = {k: str(v) if v is not None else "" for k, v in data.items()}
        pipe.hset(f"rt:fraud:{trip_id}", mapping=serialized)
        pipe.expire(f"rt:fraud:{trip_id}", self.redis._ttl)
        await pipe.execute()
