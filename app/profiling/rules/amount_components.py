AMOUNT_FORMULAS: dict[str, dict[str, str | list[str]]] = {
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

AMOUNT_TOLERANCE = 0.02
