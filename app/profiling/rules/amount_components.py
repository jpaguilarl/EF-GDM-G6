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
            # Peaje CBD (Congestion Relief Zone de la MTA) anadido por la TLC a
            # partir de 2025; es parte de total_amount. Omitirlo hacia que la
            # correccion de accuracy restara ~0.75 USD a cada viaje de 2025.
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
            # Peaje CBD (Congestion Relief Zone de la MTA), TLC 2025+; parte de
            # total_amount (ver nota en la formula de yellow).
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

AMOUNT_TOLERANCE = 0.02
