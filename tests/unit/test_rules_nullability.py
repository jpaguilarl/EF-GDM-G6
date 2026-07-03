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
    assert "airport_fee" in nc
    assert "congestion_surcharge" in nc
    assert "cbd_congestion_fee" in nc  # peaje CBD MTA (2025+)
    assert len(nc) == 3


def test_green_nullable():
    nc = NULLABLE_COLUMNS["green"]
    assert "ehail_fee" in nc
    assert "congestion_surcharge" in nc
    assert "cbd_congestion_fee" in nc  # peaje CBD MTA (2025+)
    assert "airport_fee" not in nc
    assert len(nc) == 3


def test_fhv_nullable():
    nc = NULLABLE_COLUMNS["fhv"]
    assert "SR_Flag" in nc
    assert len(nc) == 1


def test_fhvhv_nullable():
    nc = NULLABLE_COLUMNS["fhvhv"]
    expected = {
        "originating_base_num",
        "on_scene_datetime",
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag",
    }
    assert nc == expected
    assert len(nc) == 7
