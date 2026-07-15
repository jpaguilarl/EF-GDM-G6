from datetime import datetime

from app.schemas.settings_schema import SpeedConfig
from app.speed.redis_client import RedisClient
from app.speed.schema import EnrichedRide


class RealtimeAggregator:
    """EventBus subscriber. O(1) Redis updates per event."""

    def __init__(self, redis: RedisClient, config: SpeedConfig):
        self.redis = redis
        self.config = config
        self._block_seconds = config.block_minutes * 60

    async def on_event(self, ride: EnrichedRide) -> None:
        if not await self._update_uniqueness(ride):
            return
        await self._update_demand_volume(ride)
        await self._update_financial_performance(ride)
        await self._update_operational_profile(ride)
        await self._update_supply_demand(ride)
        await self._update_tipping_behavior(ride)

    async def _update_uniqueness(self, ride: EnrichedRide) -> bool:
        key = f"rt:seen:{ride.trip_id}"
        result = await self.redis.redis.set(key, "1", nx=True, ex=self.redis._ttl)
        return result is not None

    async def _update_demand_volume(self, ride: EnrichedRide) -> None:
        key = f"rt:dv:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.pickup_hour}:{ride.pu_location_id}"
        pipe = self.redis.redis.pipeline()
        pipe.hincrby(key, "viajes", 1)
        pipe.expire(key, self.redis._ttl)
        await pipe.execute()

    async def _update_financial_performance(self, ride: EnrichedRide) -> None:
        key = f"rt:fp:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"
        pipe = self.redis.redis.pipeline()
        pipe.hincrby(key, "viajes", 1)

        if ride.service_id in ("yellow", "green"):
            if ride.fare_amount is not None:
                pipe.hincrbyfloat(key, "fare_amount", ride.fare_amount)
            if ride.extra is not None:
                pipe.hincrbyfloat(key, "extra", ride.extra)
            if ride.mta_tax is not None:
                pipe.hincrbyfloat(key, "mta_tax", ride.mta_tax)
            if ride.tip_amount is not None:
                pipe.hincrbyfloat(key, "tip_amount", ride.tip_amount)
            if ride.tolls_amount is not None:
                pipe.hincrbyfloat(key, "tolls_amount", ride.tolls_amount)
            if ride.total_amount is not None:
                pipe.hincrbyfloat(key, "total_amount", ride.total_amount)
        elif ride.service_id == "fhvhv":
            if ride.base_passenger_fare is not None:
                pipe.hincrbyfloat(key, "base_passenger_fare", ride.base_passenger_fare)
            if ride.tolls_amount is not None:
                pipe.hincrbyfloat(key, "tolls", ride.tolls_amount)
            if ride.tips is not None:
                pipe.hincrbyfloat(key, "tips", ride.tips)
            if ride.driver_pay is not None:
                pipe.hincrbyfloat(key, "driver_pay", ride.driver_pay)

        pipe.expire(key, self.redis._ttl)
        await pipe.execute()

    async def _update_operational_profile(self, ride: EnrichedRide) -> None:
        key = f"rt:op:{ride.service_id}:{ride.pickup_datetime.date()}:{ride.bloque_horario}:{ride.pu_location_id}"
        pipe = self.redis.redis.pipeline()
        pipe.hincrby(key, "viajes", 1)
        if ride.trip_duration_minutes is not None:
            pipe.hincrbyfloat(key, "duracion_total_min", ride.trip_duration_minutes)
        if ride.trip_distance is not None:
            pipe.hincrbyfloat(key, "distancia_total_millas", ride.trip_distance)
        if ride.trip_miles is not None:
            pipe.hincrbyfloat(key, "millas_validas_sum", ride.trip_miles)
        if ride.trip_duration_minutes is not None:
            pipe.hincrbyfloat(key, "horas_validas_sum", ride.trip_duration_minutes / 60.0)
        if ride.shared_request_flag is not None:
            pipe.hincrby(key, "solicitud_compartida", 1)
        if ride.shared_match_flag is not None:
            pipe.hincrby(key, "match_compartido", 1)
        pipe.expire(key, self.redis._ttl)
        await pipe.execute()

    async def _update_supply_demand(self, ride: EnrichedRide) -> None:
        step = self._block_seconds
        ts = ride.pickup_datetime.timestamp()
        block_unix = int(ts // step * step)

        if ride.pu_location_id is not None:
            pu_key = f"rt:sd:{ride.pu_location_id}:{block_unix}"
            pipe = self.redis.redis.pipeline()
            pipe.hincrby(pu_key, "salientes", 1)
            if ride.pu_borough:
                pipe.hset(pu_key, "borough", ride.pu_borough)
            pipe.expire(pu_key, self.redis._ttl)
            await pipe.execute()

        if ride.do_location_id is not None:
            do_key = f"rt:sd:{ride.do_location_id}:{block_unix}"
            pipe = self.redis.redis.pipeline()
            pipe.hincrby(do_key, "entrantes", 1)
            if ride.do_borough:
                pipe.hset(do_key, "borough", ride.do_borough)
            pipe.expire(do_key, self.redis._ttl)
            await pipe.execute()

    async def _update_tipping_behavior(self, ride: EnrichedRide) -> None:
        pu_borough = ride.pu_borough or "Desconocido"
        do_borough = ride.do_borough or "Desconocido"
        payment = ride.payment_type_id or 0
        generosidad = ride.categoria_generosidad or "Sin Propina"
        key = f"rt:tb:{ride.service_id}:{ride.pickup_datetime.date()}:{pu_borough}:{do_borough}:{payment}:{generosidad}"
        pipe = self.redis.redis.pipeline()
        pipe.hincrby(key, "viajes", 1)
        if ride.tip_amount is not None:
            pipe.hincrbyfloat(key, "propina_total", ride.tip_amount)
        if ride.fare_amount is not None:
            pipe.hincrbyfloat(key, "tarifa_base_sum", ride.fare_amount)
            if ride.tip_amount is not None:
                pipe.hincrbyfloat(key, "propina_base_sum", ride.tip_amount)
        if ride.trip_distance is not None:
            pipe.hincrbyfloat(key, "millas_sum", ride.trip_distance)
        pipe.expire(key, self.redis._ttl)
        await pipe.execute()
