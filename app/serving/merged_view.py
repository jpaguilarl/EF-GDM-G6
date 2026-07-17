from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl
import redis.exceptions

from app.serving.query_engine import PolarsQueryEngine
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


def _dv_key_to_row(parts: list[str], hvals: dict[str, str]) -> dict[str, Any]:
    return {
        "service_id": parts[0],
        "fecha_viaje": parts[1],
        "pickup_hour": int(parts[2]),
        "pu_location_id": int(parts[3]),
        "viajes": int(hvals.get("viajes", 0)),
    }


def _fp_key_to_row(parts: list[str], hvals: dict[str, str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service_id": parts[0],
        "fecha_viaje": parts[1],
        "bloque_horario": parts[2],
        "pu_location_id": int(parts[3]),
    }
    _add_measures(hvals, row, {
        "viajes": int, "fare_amount": float, "extra": float, "mta_tax": float,
        "tip_amount": float, "tolls_amount": float, "total_amount": float,
        "base_passenger_fare": float, "tolls": float, "tips": float, "driver_pay": float,
    })
    return row


def _op_key_to_row(parts: list[str], hvals: dict[str, str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service_id": parts[0],
        "fecha_viaje": parts[1],
        "bloque_horario": parts[2],
        "pu_location_id": int(parts[3]),
    }
    _add_measures(hvals, row, {
        "viajes": int, "duracion_total_min": float, "distancia_total_millas": float,
        "millas_validas_sum": float, "horas_validas_sum": float,
        "solicitud_compartida": int, "match_compartido": int,
    })
    return row


def _sd_key_to_row(parts: list[str], hvals: dict[str, str]) -> dict[str, Any]:
    loc_id = int(parts[0])
    block_unix = int(parts[1])
    bloque_t = datetime.fromtimestamp(block_unix)
    entrantes = int(hvals.get("entrantes", 0))
    salientes = int(hvals.get("salientes", 0))
    flujo = salientes - entrantes
    return {
        "location_id": loc_id,
        "bloque_temporal_t": bloque_t.isoformat(),
        "borough": hvals.get("borough"),
        "entrantes": entrantes,
        "salientes": salientes,
        "flujo_neto_oferta": flujo,
        "deficit_severo_flag": flujo < -10,
    }


def _tb_key_to_row(parts: list[str], hvals: dict[str, str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service_id": parts[0],
        "fecha_viaje": parts[1],
        "pu_borough": parts[2],
        "do_borough": parts[3],
        "payment_type_id": int(parts[4]),
        "categoria_generosidad": parts[5],
    }
    _add_measures(hvals, row, {
        "viajes": int, "propina_total": float, "tarifa_base_sum": float,
        "propina_base_sum": float, "millas_sum": float,
    })
    return row


def _add_measures(hvals: dict[str, str], row: dict[str, Any], measures: dict[str, type]) -> None:
    for col, typ in measures.items():
        val = hvals.get(col)
        if val is not None:
            row[col] = typ(val)


def dv_key_from_ride(ride: EnrichedRide) -> str:
    return f"rt:dv:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pickup_hour}:{ride.pu_location_id}"


def fp_key_from_ride(ride: EnrichedRide) -> str:
    return f"rt:fp:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"


def op_key_from_ride(ride: EnrichedRide) -> str:
    return f"rt:op:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"


def sd_key_from_ride(ride: EnrichedRide, block_seconds: int = 900) -> str:
    block = int(ride.pickup_datetime.timestamp() // block_seconds * block_seconds)
    return f"rt:sd:{ride.pu_location_id}:{block}"


def tb_key_from_ride(ride: EnrichedRide) -> str:
    pu_borough = ride.pu_borough or "Desconocido"
    do_borough = ride.do_borough or "Desconocido"
    payment = ride.payment_type_id or 0
    generosidad = ride.categoria_generosidad or "Sin Propina"
    return f"rt:tb:{ride.service_id}:{ride.pickup_datetime.date()}:{pu_borough}:{do_borough}:{payment}:{generosidad}"


MART_CONFIGS: dict[str, dict[str, Any]] = {
    "mart_demand_volume": {
        "prefix": "rt:dv:",
        "num_key_parts": 4,
        "key_to_row": _dv_key_to_row,
        "ride_to_key": dv_key_from_ride,
        "dedup_key": ["service_id", "fecha_viaje", "pickup_hour", "pu_location_id"],
    },
    "mart_financial_performance": {
        "prefix": "rt:fp:",
        "num_key_parts": 4,
        "key_to_row": _fp_key_to_row,
        "ride_to_key": fp_key_from_ride,
        "dedup_key": ["service_id", "fecha_viaje", "bloque_horario", "pu_location_id"],
    },
    "mart_operational_profile": {
        "prefix": "rt:op:",
        "num_key_parts": 4,
        "key_to_row": _op_key_to_row,
        "ride_to_key": op_key_from_ride,
        "dedup_key": ["service_id", "fecha_viaje", "bloque_horario", "pu_location_id"],
    },
    "mart_supply_demand_balance": {
        "prefix": "rt:sd:",
        "num_key_parts": 2,
        "key_to_row": _sd_key_to_row,
        "ride_to_key": sd_key_from_ride,
        "dedup_key": ["location_id", "bloque_temporal_t"],
    },
    "mart_tipping_behavior": {
        "prefix": "rt:tb:",
        "num_key_parts": 6,
        "key_to_row": _tb_key_to_row,
        "ride_to_key": tb_key_from_ride,
        "dedup_key": ["service_id", "fecha_viaje", "pu_borough", "do_borough", "payment_type_id", "categoria_generosidad"],
    },
}


class MergedViewReader:
    def __init__(self, engine: PolarsQueryEngine, redis: RedisClient, block_minutes: int = 15):
        self.engine = engine
        self.redis = redis
        self.block_minutes = block_minutes

    async def read_merged(
        self,
        mart: str,
        time_column: str | None,
        filter_cols: dict[str, Any] | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        if filter_cols is None:
            filter_cols = {}

        batch_df = self.engine.query(mart, filters=filter_cols, limit=limit + 1000)
        batch_rows = batch_df.to_dicts()

        if mart == "mart_abc_xyz_zones":
            redis_rows = await self._get_abc_xyz_redis_rows(filter_cols)
        else:
            redis_rows = await self._get_redis_rows(mart, filter_cols)

        config = MART_CONFIGS.get(mart)
        dedup_keys = config["dedup_key"] if config else []

        merged: dict[tuple, dict] = {}
        for row in batch_rows:
            k = tuple(row.get(c) for c in dedup_keys) if dedup_keys else id(row)
            merged[k] = row
        for row in redis_rows:
            k = tuple(row.get(c) for c in dedup_keys) if dedup_keys else id(row)
            if k not in merged:
                merged[k] = row

        result = list(merged.values())
        if result and time_column is not None and time_column in result[0]:
            result.sort(key=lambda r: str(r.get(time_column, "")), reverse=True)
        return result[:limit]

    async def get_realtime_row(self, mart: str, ride: EnrichedRide) -> dict[str, Any] | None:
        if mart == "mart_abc_xyz_zones":
            return None
        config = MART_CONFIGS.get(mart)
        if not config:
            return None
        if mart == "mart_supply_demand_balance":
            block_seconds = self.block_minutes * 60
            key = sd_key_from_ride(ride, block_seconds)
        else:
            key = config["ride_to_key"](ride)
        try:
            hvals = await self.redis.redis.hgetall(key)
        except redis.exceptions.ConnectionError:
            return None
        if not hvals:
            return None
        parts = key[len(config["prefix"]):].split(":")
        return config["key_to_row"](parts, hvals)

    async def _get_redis_rows(self, mart: str, filter_cols: dict) -> list[dict]:
        config = MART_CONFIGS.get(mart)
        if not config:
            return []
        keys = await self._scan_keys(f"{config['prefix']}*")
        rows = []
        for key in keys:
            suffix = key[len(config["prefix"]):]
            parts = suffix.split(":")
            if len(parts) != config["num_key_parts"]:
                continue
            hvals = await self.redis.redis.hgetall(key)
            row = config["key_to_row"](parts, hvals)
            if self._matches_filters(row, filter_cols):
                rows.append(row)
        return rows

    async def _get_abc_xyz_redis_rows(self, filter_cols: dict) -> list[dict]:
        dv_keys = await self._scan_keys("rt:dv:*")
        fp_keys = await self._scan_keys("rt:fp:*")

        viajes_per_zone: dict[tuple, int] = {}
        for key in dv_keys:
            parts = key[len("rt:dv:"):].split(":")
            if len(parts) == 4:
                service_id, fecha, hour, loc = parts
                loc_id = int(loc)
                h = await self.redis.redis.hgetall(key)
                k = (loc_id, service_id)
                viajes_per_zone[k] = viajes_per_zone.get(k, 0) + int(h.get("viajes", 0))

        ingresos_per_zone: dict[tuple, float] = {}
        for key in fp_keys:
            parts = key[len("rt:fp:"):].split(":")
            if len(parts) == 4:
                service_id, fecha, bloque, loc = parts
                loc_id = int(loc)
                h = await self.redis.redis.hgetall(key)
                k = (loc_id, service_id)
                total = float(h.get("total_amount", 0) or 0)
                bpf = float(h.get("base_passenger_fare", 0) or 0)
                ingresos_per_zone[k] = ingresos_per_zone.get(k, 0.0) + total + bpf

        rows = []
        for (loc_id, service_id), viajes in viajes_per_zone.items():
            ingresos = ingresos_per_zone.get((loc_id, service_id), 0.0)
            row = {
                "pu_location_id": loc_id,
                "service_id": service_id,
                "viajes_realtime": viajes,
                "ingresos_realtime": round(ingresos, 2),
            }
            if self._matches_filters(row, filter_cols):
                rows.append(row)
        return rows

    async def read_fraud(
        self,
        limit: int = 100,
        offset: int = 0,
        service_id: list[str] | None = None,
        is_fraud: bool | None = None,
        ratecode_id: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        keys = await self._scan_keys("rt:fraud:*")
        rows = []
        for key in keys:
            hvals = await self.redis.redis.hgetall(key)
            if not hvals:
                continue
            row: dict[str, Any] = {
                "trip_id": int(hvals.get("trip_id", 0)),
                "service_id": hvals.get("service_id", ""),
                "ratecode_id": int(hvals["ratecode_id"]) if hvals.get("ratecode_id", "") else None,
                "anomaly_score": float(hvals["anomaly_score"]) if hvals.get("anomaly_score", "") else None,
                "is_fraud": hvals.get("is_fraud") == "True",
                "is_anomaly_candidate": hvals.get("is_anomaly_candidate") == "True",
                "timestamp": hvals.get("timestamp", ""),
            }
            if service_id and row["service_id"] not in service_id:
                continue
            if is_fraud is not None and row["is_fraud"] != is_fraud:
                continue
            if ratecode_id and row["ratecode_id"] not in ratecode_id:
                continue
            rows.append(row)

        rows.sort(key=lambda r: str(r.get("timestamp", "")), reverse=True)
        return rows[offset:offset + limit]

    async def read_clusters(
        self,
        limit: int = 100,
        offset: int = 0,
        service_id: list[str] | None = None,
        cluster_id: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        keys = await self._scan_keys("rt:cluster:*")
        rows = []
        for key in keys:
            hvals = await self.redis.redis.hgetall(key)
            if not hvals:
                continue
            row: dict[str, Any] = {
                "trip_id": int(key.split(":")[-1]),
                "cluster_id": int(hvals.get("cluster_id", 0)),
                "service_id": hvals.get("service_id", ""),
            }
            if service_id and row["service_id"] not in service_id:
                continue
            if cluster_id and row["cluster_id"] not in cluster_id:
                continue
            rows.append(row)
        return rows[offset:offset + limit]

    async def _scan_keys(self, pattern: str) -> list[str]:
        try:
            keys = []
            cursor = 0
            while True:
                cursor, batch = await self.redis.redis.scan(cursor=cursor, match=pattern, count=1000)
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
        except redis.exceptions.ConnectionError:
            return []

    @staticmethod
    def _matches_filters(row: dict, filter_cols: dict) -> bool:
        for col, val in filter_cols.items():
            if val is None:
                continue
            if col not in row:
                return False
            if isinstance(val, list):
                if row[col] not in val:
                    return False
            elif row[col] != val:
                return False
        return True
