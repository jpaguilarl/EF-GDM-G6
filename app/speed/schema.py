from datetime import datetime

from pydantic import BaseModel


class RideEvent(BaseModel):
    service_id: str
    pickup_datetime: datetime
    dropoff_datetime: datetime | None = None
    pu_location_id: int | None = None
    do_location_id: int | None = None
    vendor_id: int | None = None
    ratecode_id: int | None = None
    payment_type_id: int | None = None
    passenger_count: int | None = None
    trip_distance: float | None = None
    fare_amount: float | None = None
    tip_amount: float | None = None
    tolls_amount: float | None = None
    total_amount: float | None = None
    extra: float | None = None
    mta_tax: float | None = None
    improvement_surcharge: float | None = None
    congestion_surcharge: float | None = None
    airport_fee: float | None = None
    hvfhs_license_num: str | None = None
    base_passenger_fare: float | None = None
    trip_miles: float | None = None
    tips: float | None = None
    driver_pay: float | None = None
    shared_request_flag: str | None = None
    shared_match_flag: str | None = None


class EnrichedRide(BaseModel):
    trip_id: int
    service_id: str
    pickup_datetime: datetime
    dropoff_datetime: datetime | None
    pu_location_id: int | None
    do_location_id: int | None
    pu_borough: str | None
    pu_zone: str | None
    do_borough: str | None
    do_zone: str | None
    bloque_horario: str
    franja_horaria: str
    dia_categoria: str
    is_weekend: bool
    pickup_hour: int
    trip_duration_minutes: float | None
    passenger_group: str
    revenue: float | None
    fare_amount: float | None
    tolls_amount: float | None
    tip_amount: float | None = None
    payment_type_id: int | None = None
    ratecode_id: int | None = None
    hvfhs_license_num: str | None = None
    trip_distance: float | None = None
    extra: float | None = None
    mta_tax: float | None = None
    total_amount: float | None = None
    base_passenger_fare: float | None = None
    tips: float | None = None
    driver_pay: float | None = None
    trip_miles: float | None = None
    shared_request_flag: str | None = None
    shared_match_flag: str | None = None
    categoria_generosidad: str | None = None
