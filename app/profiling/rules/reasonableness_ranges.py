from app.utils.settings import settings

_DEFAULT_RANGES: dict[str, dict[str, tuple[float, float]]] = {
    "yellow": {
        "passenger_count": (0, 9),
        "trip_distance": (0, 500),
        "fare_amount": (-200, 5000),
        "total_amount": (-200, 5000),
        "tip_amount": (0, 2000),
        "tolls_amount": (0, 100),
        "improvement_surcharge": (0, 5),
        "congestion_surcharge": (0, 5),
        "cbd_congestion_fee": (0, 5),
        "airport_fee": (0, 5),
        "mta_tax": (0, 5),
        "extra": (-5, 20),
    },
    "green": {
        "passenger_count": (0, 9),
        "trip_distance": (0, 500),
        "fare_amount": (-200, 5000),
        "total_amount": (-200, 5000),
        "tip_amount": (0, 2000),
        "tolls_amount": (0, 100),
        "ehail_fee": (0, 10),
        "improvement_surcharge": (0, 5),
        "congestion_surcharge": (0, 5),
        "cbd_congestion_fee": (0, 5),
        "mta_tax": (0, 5),
        "extra": (-5, 20),
    },
    "fhv": {
        "SR_Flag": (0, 1),
    },
    "fhvhv": {
        "trip_miles": (0, 500),
        "trip_time": (0, 86400),
        "base_passenger_fare": (-200, 5000),
        "driver_pay": (-200, 5000),
        "tips": (0, 2000),
        "tolls": (0, 100),
        "bcf": (0, 5),
        "sales_tax": (0, 5),
        "congestion_surcharge": (0, 5),
        "cbd_congestion_fee": (0, 5),
        "airport_fee": (0, 5),
    },
}

REASONABLENESS_RANGES: dict[str, dict[str, tuple[float, float]]] = {
    k: dict(v) for k, v in _DEFAULT_RANGES.items()
}

_ranges = settings.profiling.rules.reasonableness_ranges
if _ranges:
    for category, columns in _ranges.items():
        if category not in REASONABLENESS_RANGES:
            REASONABLENESS_RANGES[category] = {}
        for col, rng in columns.items():
            REASONABLENESS_RANGES[category][col] = (rng[0], rng[1])

MAX_TRIP_DURATION_MINUTES = (
    settings.profiling.rules.max_trip_duration_minutes
    if settings.profiling.rules.max_trip_duration_minutes is not None
    else 1440
)
