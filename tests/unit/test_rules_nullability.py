from app.profiling.rules.nullability import NULLABLE_COLUMNS


def test_categories_present():
    assert "yellow" in NULLABLE_COLUMNS
    assert "green" in NULLABLE_COLUMNS
    assert "fhv" in NULLABLE_COLUMNS
    assert "fhvhv" in NULLABLE_COLUMNS


def test_no_unexpected_categories():
    assert set(NULLABLE_COLUMNS.keys()) == {"yellow", "green", "fhv", "fhvhv"}


def test_yellow_nullable():
    nc = NULLABLE_COLUMNS["yellow"]
    expected = {
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
    }
    assert nc == expected


def test_green_nullable():
    nc = NULLABLE_COLUMNS["green"]
    expected = {
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
    }
    assert nc == expected


def test_fhv_nullable():
    nc = NULLABLE_COLUMNS["fhv"]
    expected = {"SR_Flag", "Affiliated_base_number"}
    assert nc == expected


def test_fhvhv_nullable():
    nc = NULLABLE_COLUMNS["fhvhv"]
    expected = {
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
    }
    assert nc == expected
