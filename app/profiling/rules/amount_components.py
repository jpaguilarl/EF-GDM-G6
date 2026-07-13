from app.utils.settings import settings

_DEFAULT_FORMULAS: dict[str, dict[str, str | list[str]]] = {
    "yellow": {
        "total": "total_amount",
        "components": [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
            "cbd_congestion_fee",
        ],
    },
    "green": {
        "total": "total_amount",
        "components": [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "congestion_surcharge",
            "cbd_congestion_fee",
        ],
    },
    "fhvhv": {
        "total": "driver_pay",
        "components": [
            "base_passenger_fare",
            "tolls",
            "bcf",
            "sales_tax",
            "congestion_surcharge",
            "airport_fee",
            "tips",
        ],
    },
}

AMOUNT_FORMULAS: dict[str, dict[str, str | list[str]]] = {
    k: dict(v) for k, v in _DEFAULT_FORMULAS.items()
}

_formulas = settings.profiling.rules.amount_formulas
if _formulas:
    for category, formula in _formulas.items():
        if category not in AMOUNT_FORMULAS:
            AMOUNT_FORMULAS[category] = {}
        for key, value in formula.items():
            AMOUNT_FORMULAS[category][key] = value

AMOUNT_TOLERANCE = (
    settings.profiling.rules.amount_tolerance
    if settings.profiling.rules.amount_tolerance is not None
    else 0.02
)
