from app.profiling.rules.reasonableness_ranges import (
    MAX_TRIP_DURATION_MINUTES,
    REASONABLENESS_RANGES,
)


def test_max_trip_duration():
    assert MAX_TRIP_DURATION_MINUTES == 1440


def test_categories_present():
    assert set(REASONABLENESS_RANGES.keys()) == {"yellow", "green", "fhv", "fhvhv"}


class TestYellow:
    def setup_method(self):
        self.rr = REASONABLENESS_RANGES["yellow"]

    def test_expected_keys(self):
        expected = {
            "passenger_count", "trip_distance", "fare_amount", "total_amount",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "congestion_surcharge", "airport_fee", "mta_tax", "extra",
        }
        assert set(self.rr.keys()) == expected

    def test_airport_fee_present(self):
        assert "airport_fee" in self.rr

    def test_passenger_count_boundary(self):
        assert self.rr["passenger_count"] == (0, 9)

    def test_fare_amount_boundary(self):
        assert self.rr["fare_amount"] == (-200, 5000)


class TestGreen:
    def setup_method(self):
        self.rr = REASONABLENESS_RANGES["green"]

    def test_expected_keys(self):
        expected = {
            "passenger_count", "trip_distance", "fare_amount", "total_amount",
            "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
            "congestion_surcharge", "mta_tax", "extra",
        }
        assert set(self.rr.keys()) == expected

    def test_airport_fee_not_present(self):
        assert "airport_fee" not in self.rr

    def test_ehail_fee_present(self):
        assert "ehail_fee" in self.rr


class TestFhv:
    def setup_method(self):
        self.rr = REASONABLENESS_RANGES["fhv"]

    def test_only_sr_flag(self):
        assert self.rr == {"SR_Flag": (0, 1)}


class TestFhvhv:
    def setup_method(self):
        self.rr = REASONABLENESS_RANGES["fhvhv"]

    def test_expected_keys(self):
        expected = {
            "trip_miles", "trip_time", "base_passenger_fare", "driver_pay",
            "tips", "tolls", "bcf", "sales_tax", "congestion_surcharge",
            "airport_fee",
        }
        assert set(self.rr.keys()) == expected

    def test_trip_miles_boundary(self):
        assert self.rr["trip_miles"] == (0, 500)

    def test_driver_pay_boundary(self):
        assert self.rr["driver_pay"] == (-200, 5000)

    def test_base_passenger_fare(self):
        assert self.rr["base_passenger_fare"] == (-200, 5000)
