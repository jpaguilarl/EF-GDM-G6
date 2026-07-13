import numpy as np


class TripProfiler:
    YELLOW_GREEN_FEATURES = [
        "borough_pu",
        "borough_do",
        "franja_horaria",
        "dia_categoria",
        "payment_type",
        "ratecode",
        "passenger_group",
    ]
    FHVHV_FEATURES = [
        "borough_pu",
        "borough_do",
        "franja_horaria",
        "dia_categoria",
        "hvfhs_license_num",
    ]

    def __init__(self, model_loader, redis):
        self.models = model_loader
        self.redis = redis

    async def on_event(self, ride) -> None:
        model = self.models.kmodes_models.get(ride.service_id)
        if model is None:
            return

        features = self._extract_features(ride)
        if features is None:
            return

        mapping = self.models.kmodes_mappings.get(ride.service_id, {})
        encoded = []
        for col, val in features.items():
            code = self._encode(mapping, col, val)
            encoded.append(code)

        X = np.array([encoded], dtype=np.int32)
        cluster_id = int(model.predict(X)[0])

        await self.redis.redis.hset(
            f"rt:cluster:{ride.trip_id}",
            mapping={"cluster_id": cluster_id, "service_id": ride.service_id},
        )

    def _extract_features(self, ride) -> dict | None:
        if ride.service_id == "fhvhv":
            cols = self.FHVHV_FEATURES
        else:
            cols = self.YELLOW_GREEN_FEATURES
        vals = {}
        for col in cols:
            v = getattr(ride, self._attr_for(col), None)
            if v is None:
                return None
            vals[col] = str(v)
        return vals

    def _attr_for(self, col: str) -> str:
        mapping = {
            "borough_pu": "pu_borough",
            "borough_do": "do_borough",
            "franja_horaria": "franja_horaria",
            "dia_categoria": "dia_categoria",
            "payment_type": "payment_type_id",
            "ratecode": "ratecode_id",
            "passenger_group": "passenger_group",
            "hvfhs_license_num": "hvfhs_license_num",
        }
        return mapping.get(col, col)

    def _encode(self, mapping: dict, col: str, val: str) -> int:
        col_map = mapping.get(col, {})
        for code, label in col_map.items():
            if label == val:
                return int(code)
        return -1
