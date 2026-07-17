from app.schemas.settings_schema import (
    StorageConfig,
    AbcXyzConfig,
    DatasetsConfig,
    GenerosityConfig,
    GoldConfig,
    IsolationFraudConfig,
    KmodesConfig,
    Module,
    SariMaxConfig,
    SettingsSchema,
    SupplyDemandConfig,
)


def test_datasets_with_years():
    cfg = DatasetsConfig(years=[2025])
    assert cfg.years == [2025]


def test_datasets_with_modules():
    cfg = DatasetsConfig(years=[Module(category="yellow", year=2023, month=1)])
    assert len(cfg.years) == 1
    m = cfg.years[0]
    assert m.category == "yellow"
    assert m.year == 2023
    assert m.month == 1


def test_gold_config_defaults():
    cfg = GoldConfig()
    assert cfg.mode == "full"
    assert isinstance(cfg.supply_demand, SupplyDemandConfig)
    assert isinstance(cfg.abc_xyz, AbcXyzConfig)
    assert isinstance(cfg.generosity, GenerosityConfig)
    assert isinstance(cfg.isolation_fraud, IsolationFraudConfig)
    assert isinstance(cfg.sarimax, SariMaxConfig)
    assert isinstance(cfg.kmodes, KmodesConfig)


def test_supply_demand_default():
    cfg = SupplyDemandConfig()
    assert cfg.block_minutes == 15
    assert cfg.deficit_threshold == -10


def test_abc_xyz_default():
    cfg = AbcXyzConfig()
    assert cfg.class_a_pct == 0.80
    assert cfg.class_b_pct == 0.15
    assert cfg.xyz_x_max == 0.2
    assert cfg.xyz_y_max == 0.5


def test_generosity_default():
    cfg = GenerosityConfig()
    assert cfg.standard_low == 10.0
    assert cfg.standard_high == 18.0


def test_isolation_fraud_default():
    cfg = IsolationFraudConfig()
    assert cfg.max_samples == "auto"
    assert cfg.contamination == 0.05
    assert cfg.n_estimators == 100
    assert cfg.random_state == 42
    assert cfg.min_rows_per_ratecode == 200


def test_sarimax_default():
    cfg = SariMaxConfig()
    assert cfg.order == [1, 1, 1]
    assert cfg.seasonal_order == [1, 1, 1, 24]
    assert cfg.min_rows_per_segment == 1000
    assert cfg.forecast_horizon_hours == 168
    assert cfg.forecast_until_year is None


def test_sarimax_forecast_until_year():
    cfg = SariMaxConfig(forecast_until_year=2027)
    assert cfg.forecast_until_year == 2027

    cfg = SariMaxConfig(forecast_until_year=None)
    assert cfg.forecast_until_year is None


def test_kmodes_default():
    cfg = KmodesConfig()
    assert cfg.init_method == "Cao"
    assert cfg.max_k == 8
    assert cfg.n_init == 2
    assert cfg.random_state == 42


def test_settings_schema_full():
    cfg = SettingsSchema(
        storage=StorageConfig(),
        datasets=DatasetsConfig(years=[2025]),
        gold=GoldConfig(
            mode="incremental",
            supply_demand=SupplyDemandConfig(block_minutes=30, deficit_threshold=-5),
            isolation_fraud=IsolationFraudConfig(contamination=0.1),
        ),
    )
    assert cfg.datasets.years == [2025]
    assert cfg.gold.mode == "incremental"
    assert cfg.gold.supply_demand.block_minutes == 30
    assert cfg.gold.isolation_fraud.contamination == 0.1
    assert cfg.gold.isolation_fraud.max_samples == "auto"


def test_settings_schema_default_gold():
    cfg = SettingsSchema(storage=StorageConfig(), datasets=DatasetsConfig(years=[2023]))
    assert cfg.gold.mode == "full"
    assert cfg.datasets.years == [2023]
