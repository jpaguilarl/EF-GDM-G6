from app.profiling.rules.amount_components import AMOUNT_FORMULAS, AMOUNT_TOLERANCE


def test_amount_tolerance():
    assert AMOUNT_TOLERANCE == 0.02


def test_categories_present():
    assert set(AMOUNT_FORMULAS.keys()) == {"yellow", "green", "fhvhv"}


class TestYellow:
    def setup_method(self):
        self.cfg = AMOUNT_FORMULAS["yellow"]

    def test_total_is_total_amount(self):
        assert self.cfg["total"] == "total_amount"

    def test_components(self):
        expected = [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
        ]
        assert self.cfg["components"] == expected
        assert len(self.cfg["components"]) == 8


class TestGreen:
    def setup_method(self):
        self.cfg = AMOUNT_FORMULAS["green"]

    def test_total_is_total_amount(self):
        assert self.cfg["total"] == "total_amount"

    def test_components(self):
        expected = [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "congestion_surcharge",
        ]
        assert self.cfg["components"] == expected
        assert len(self.cfg["components"]) == 8


class TestFhvhv:
    def setup_method(self):
        self.cfg = AMOUNT_FORMULAS["fhvhv"]

    def test_total_is_driver_pay_not_total_amount(self):
        assert self.cfg["total"] == "driver_pay"
        assert self.cfg["total"] != "total_amount"

    def test_components(self):
        expected = [
            "base_passenger_fare",
            "tolls",
            "bcf",
            "sales_tax",
            "congestion_surcharge",
            "airport_fee",
            "tips",
        ]
        assert self.cfg["components"] == expected
        assert len(self.cfg["components"]) == 7
