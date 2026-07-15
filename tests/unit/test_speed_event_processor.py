from datetime import datetime, timedelta

import pytest
import xxhash

from app.schemas.settings_schema import SpeedConfig
from app.speed.event_processor import EventProcessor
from app.speed.schema import RideEvent
from app.speed.zone_lookup import ZoneLookup


@pytest.fixture
def zone_lookup() -> ZoneLookup:
    zl = ZoneLookup()
    zl._zones = {
        237: {"borough": "Manhattan", "zone": "Midtown", "borough_es": "Manhattan"},
        238: {"borough": "Brooklyn", "zone": "Williamsburg", "borough_es": "Brooklyn"},
    }
    return zl


@pytest.fixture
def config() -> SpeedConfig:
    return SpeedConfig()


@pytest.fixture
def processor(zone_lookup: ZoneLookup, config: SpeedConfig) -> EventProcessor:
    return EventProcessor(zone_lookup, config)


class TestRejectRules:
    def test_completeness_missing_service_id(self, processor: EventProcessor):
        event = RideEvent(service_id="yellow", pickup_datetime=datetime.now())
        setattr(event, "vendor_id", None)
        result = processor.process(event)
        assert result is None

    def test_completeness_missing_vendor_id_yellow(self, processor: EventProcessor):
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=datetime.now(),
            vendor_id=None,
        )
        result = processor.process(event)
        assert result is None

    def test_completeness_missing_hvfhs_license_fhvhv(self, processor: EventProcessor):
        event = RideEvent(
            service_id="fhvhv",
            pickup_datetime=datetime.now(),
            hvfhs_license_num=None,
        )
        result = processor.process(event)
        assert result is None

    def test_timeliness_outside_window(self, processor: EventProcessor):
        far_future = datetime.now() + timedelta(days=365)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=far_future,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is None

    def test_timeliness_inside_window(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None

    def test_datetime_order_dropoff_before_pickup(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now - timedelta(minutes=10),
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is None

    def test_datetime_order_valid(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=15),
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None

    def test_zone_validity_unknown_pu(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            vendor_id=1,
            pu_location_id=999,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is None

    def test_zone_validity_unknown_do(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=999,
        )
        result = processor.process(event)
        assert result is None

    def test_valid_event_passes_all_rules(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=15),
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
            passenger_count=2,
            fare_amount=15.0,
            tolls_amount=2.5,
            total_amount=20.0,
        )
        result = processor.process(event)
        assert result is not None
        assert result.trip_id > 0

    def test_fhvhv_valid_event(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="fhvhv",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=20),
            hvfhs_license_num="HV0001",
            pu_location_id=237,
            do_location_id=238,
            base_passenger_fare=25.0,
        )
        result = processor.process(event)
        assert result is not None
        assert result.trip_id > 0


class TestEnrichment:
    def test_bloque_horario(self, processor: EventProcessor):
        pickup = datetime.now().replace(hour=3)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.bloque_horario == "Madrugada"

    def test_franja_horaria(self, processor: EventProcessor):
        pickup = datetime.now().replace(hour=10)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.franja_horaria == "Mañana"

    def test_dia_categoria(self, processor: EventProcessor):
        pickup = datetime.now().replace(hour=10)
        iso = pickup.isoweekday()
        expected = "Fin de Semana" if iso >= 6 else "Día Laborable"
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.dia_categoria == expected

    def test_is_weekend(self, processor: EventProcessor):
        pickup = datetime.now().replace(hour=10)
        iso = pickup.isoweekday()
        expected = iso >= 6
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.is_weekend == expected

    def test_passenger_group(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
            passenger_count=1,
        )
        result = processor.process(event)
        assert result is not None
        assert result.passenger_group == "Solo"

    def test_passenger_group_none(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
            passenger_count=None,
        )
        result = processor.process(event)
        assert result is not None
        assert result.passenger_group == "Desconocido"

    def test_duration_computed(self, processor: EventProcessor):
        pickup = datetime.now()
        dropoff = pickup + timedelta(minutes=30)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            dropoff_datetime=dropoff,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.trip_duration_minutes == pytest.approx(30.0, rel=0.01)

    def test_duration_none_when_no_dropoff(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.trip_duration_minutes is None

    def test_revenue_yellow(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
            total_amount=35.0,
        )
        result = processor.process(event)
        assert result is not None
        assert result.revenue == 35.0

    def test_revenue_fhvhv(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="fhvhv",
            pickup_datetime=pickup,
            hvfhs_license_num="HV0001",
            pu_location_id=237,
            do_location_id=238,
            base_passenger_fare=25.0,
        )
        result = processor.process(event)
        assert result is not None
        assert result.revenue == 25.0

    def test_revenue_fhv_none(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="fhv",
            pickup_datetime=pickup,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.revenue is None

    def test_zone_enrichment(self, processor: EventProcessor):
        pickup = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.pu_borough == "Manhattan"
        assert result.pu_zone == "Midtown"
        assert result.do_borough == "Brooklyn"
        assert result.do_zone == "Williamsburg"

    def test_pickup_hour(self, processor: EventProcessor):
        pickup = datetime.now().replace(hour=14, minute=30)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        result = processor.process(event)
        assert result is not None
        assert result.pickup_hour == 14


class TestTripId:
    def test_trip_id_deterministic(self, processor: EventProcessor):
        now = datetime.now()
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=10),
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        r1 = processor.process(event)
        r2 = processor.process(event)
        assert r1 is not None and r2 is not None
        assert r1.trip_id == r2.trip_id

    def test_trip_id_different_for_different_events(self, processor: EventProcessor):
        now = datetime.now()
        e1 = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=10),
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
        )
        e2 = RideEvent(
            service_id="yellow",
            pickup_datetime=now,
            dropoff_datetime=now + timedelta(minutes=10),
            vendor_id=2,
            pu_location_id=237,
            do_location_id=238,
        )
        r1 = processor.process(e1)
        r2 = processor.process(e2)
        assert r1 is not None and r2 is not None
        assert r1.trip_id != r2.trip_id

    def test_trip_id_parity_with_xxhash(self, processor: EventProcessor):
        pickup = datetime.now()
        dropoff = pickup + timedelta(minutes=15)
        event = RideEvent(
            service_id="yellow",
            pickup_datetime=pickup,
            dropoff_datetime=dropoff,
            vendor_id=1,
            pu_location_id=237,
            do_location_id=238,
            total_amount=20.0,
        )
        result = processor.process(event)
        assert result is not None

        values = ["1", str(pickup), str(dropoff), "237"]
        expected = xxhash.xxh64("||".join(values), seed=0).intdigest()
        assert result.trip_id == expected

    def test_trip_id_fhvhv(self, processor: EventProcessor):
        pickup = datetime.now()
        dropoff = pickup + timedelta(minutes=30)
        event = RideEvent(
            service_id="fhvhv",
            pickup_datetime=pickup,
            dropoff_datetime=dropoff,
            hvfhs_license_num="HV0001",
            pu_location_id=237,
            do_location_id=238,
            base_passenger_fare=30.0,
        )
        result = processor.process(event)
        assert result is not None
        assert result.trip_id > 0

        values = ["HV0001", "", str(pickup), str(dropoff)]
        expected = xxhash.xxh64("||".join(values), seed=0).intdigest()
        assert result.trip_id == expected
