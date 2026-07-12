from app.utils.settings import settings

# Columns listed here are NULLABLE: nulls pass through unchanged.
# Columns NOT listed here are REQUIRED: a null triggers row rejection.
# Override per-category via config.yaml → profiling.rules.nullability.
_DEFAULT_NULLABLE: dict[str, set[str]] = {
    "fhv": {"SR_Flag", "Affiliated_base_number"},
    "fhvhv": {
        "originating_base_num",
        "on_scene_datetime",
        "request_datetime",
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "tips",
    },
    "yellow": {
        "RatecodeID",
        "store_and_fwd_flag",
        "payment_type",
        "tip_amount",
        "tolls_amount",
        "extra",
        "mta_tax",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    },
    "green": {
        "RatecodeID",
        "store_and_fwd_flag",
        "payment_type",
        "tip_amount",
        "tolls_amount",
        "extra",
        "mta_tax",
        "ehail_fee",
        "improvement_surcharge",
        "congestion_surcharge",
        "cbd_congestion_fee",
        "trip_type",
    },
}

NULLABLE_COLUMNS: dict[str, set[str]] = dict(_DEFAULT_NULLABLE)

_nullability = settings.profiling.rules.nullability
if _nullability:
    for k, v in _nullability.items():
        NULLABLE_COLUMNS[k] = set(v)
