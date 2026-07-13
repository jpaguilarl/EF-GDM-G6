from datetime import datetime, timedelta

import xxhash

from app.pipeline.silver import SilverCleaner
from app.pipeline.gold_impl.feature_rules import time_blocks as tb
from app.pipeline.gold_impl.feature_rules import passenger_groups as pg
from app.schemas.settings_schema import SpeedConfig
from app.speed.schema import EnrichedRide, RideEvent
from app.speed.zone_lookup import ZoneLookup

COLUMN_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pu_location_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "hvfhs_license_num": "hvfhs_license_num",
    "request_datetime": "request_datetime",
    "pickup_datetime": "pickup_datetime",
    "dropoff_datetime": "dropoff_datetime",
    "dropOff_datetime": "dropoff_datetime",
    "dispatching_base_num": "dispatching_base_num",
}

UNION_REVENUE_COL = {
    "yellow": "total_amount",
    "green": "total_amount",
    "fhvhv": "base_passenger_fare",
}

REQUIRED_FIELDS = {
    "yellow": ["service_id", "pickup_datetime", "vendor_id"],
    "green": ["service_id", "pickup_datetime", "vendor_id"],
    "fhvhv": ["service_id", "pickup_datetime", "hvfhs_license_num"],
    "fhv": ["service_id", "pickup_datetime"],
}


class EventProcessor:
    REJECT_RULES = [
        "_check_completeness",
        "_check_timeliness",
        "_check_datetime_order",
        "_check_zone_validity",
    ]

    def __init__(self, zone_lookup: ZoneLookup, config: SpeedConfig):
        self.zone = zone_lookup
        self.config = config

    def process(self, event: RideEvent) -> EnrichedRide | None:
        for rule in self.REJECT_RULES:
            if not getattr(self, rule)(event):
                return None
        return self._enrich(event)

    def _check_completeness(self, event: RideEvent) -> bool:
        required = REQUIRED_FIELDS.get(event.service_id, ["service_id", "pickup_datetime"])
        for field_name in required:
            if getattr(event, field_name, None) is None:
                return False
        return True

    def _check_timeliness(self, event: RideEvent) -> bool:
        now = datetime.now()
        year, month = now.year, now.month

        month_start = datetime(year, month, 1)
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1)
        else:
            next_month_start = datetime(year, month + 1, 1)

        lower = month_start - timedelta(days=1)
        upper = next_month_start + timedelta(days=1)
        return lower <= event.pickup_datetime < upper

    def _check_datetime_order(self, event: RideEvent) -> bool:
        if event.dropoff_datetime is None:
            return True
        return event.dropoff_datetime >= event.pickup_datetime

    def _check_zone_validity(self, event: RideEvent) -> bool:
        if event.pu_location_id is not None and self.zone.lookup(event.pu_location_id) is None:
            return False
        if event.do_location_id is not None and self.zone.lookup(event.do_location_id) is None:
            return False
        return True

    def _enrich(self, event: RideEvent) -> EnrichedRide:
        trip_id = self._compute_trip_id(event)

        pu = self.zone.lookup(event.pu_location_id)
        do = self.zone.lookup(event.do_location_id)

        pickup = event.pickup_datetime
        hour = pickup.hour
        iso_dow = pickup.isoweekday()

        bloque = tb.bloque_horario_py(hour)
        franja = tb.franja_horaria_py(hour)
        dia_cat = tb.dia_categoria_py(iso_dow)
        weekend = tb.is_weekend_py(iso_dow)

        duration = None
        if event.dropoff_datetime and event.dropoff_datetime > pickup:
            duration = (event.dropoff_datetime - pickup).total_seconds() / 60.0

        revenue = self._normalize_revenue(event)
        pgroup = pg.passenger_group_py(event.passenger_count)

        return EnrichedRide(
            trip_id=trip_id,
            service_id=event.service_id,
            pickup_datetime=pickup,
            dropoff_datetime=event.dropoff_datetime,
            pu_location_id=event.pu_location_id,
            do_location_id=event.do_location_id,
            pu_borough=pu["borough"] if pu else None,
            pu_zone=pu["zone"] if pu else None,
            do_borough=do["borough"] if do else None,
            do_zone=do["zone"] if do else None,
            bloque_horario=bloque,
            franja_horaria=franja,
            dia_categoria=dia_cat,
            is_weekend=weekend,
            pickup_hour=hour,
            trip_duration_minutes=duration,
            passenger_group=pgroup,
            revenue=revenue,
            fare_amount=event.fare_amount,
            tolls_amount=event.tolls_amount,
        )

    def _compute_trip_id(self, event: RideEvent) -> int:
        keys = SilverCleaner.COMPOSITE_KEYS.get(event.service_id, [])
        values = []
        for col in keys:
            val = self._resolve_column(event, col)
            values.append(str(val) if val is not None else "")
        h = xxhash.xxh64("||".join(values), seed=0)
        return h.intdigest()

    def _resolve_column(self, event: RideEvent, source_col: str) -> str | None:
        event_field = COLUMN_MAP.get(source_col)
        if event_field is None:
            return None
        return getattr(event, event_field, None)

    def _normalize_revenue(self, event: RideEvent) -> float | None:
        rev_col = UNION_REVENUE_COL.get(event.service_id)
        if rev_col is None:
            return None
        return getattr(event, rev_col, None)
