from pydantic import BaseModel, Field


class Module(BaseModel):
    category: str
    year: int
    month: int


class DatasetsConfig(BaseModel):
    years: list[int | Module]


class SupplyDemandConfig(BaseModel):
    block_minutes: int = 15
    deficit_threshold: int = -10


class AbcXyzConfig(BaseModel):
    class_a_pct: float = 0.80
    class_b_pct: float = 0.15
    xyz_x_max: float = 0.2
    xyz_y_max: float = 0.5


class GenerosityConfig(BaseModel):
    standard_low: float = 10.0
    standard_high: float = 18.0


class IsolationFraudConfig(BaseModel):
    contamination: float = 0.05
    n_estimators: int = 100
    max_samples: str = "auto"
    random_state: int = 42
    min_rows_per_ratecode: int = 200


class SariMaxConfig(BaseModel):
    order: list[int] = [1, 1, 1]
    seasonal_order: list[int] = [1, 1, 1, 24]
    min_rows_per_segment: int = 1000
    forecast_horizon_hours: int = 168


class GoldConfig(BaseModel):
    mode: str = "full"
    supply_demand: SupplyDemandConfig = Field(default_factory=SupplyDemandConfig)
    abc_xyz: AbcXyzConfig = Field(default_factory=AbcXyzConfig)
    generosity: GenerosityConfig = Field(default_factory=GenerosityConfig)
    isolation_fraud: IsolationFraudConfig = Field(default_factory=IsolationFraudConfig)
    sarimax: SariMaxConfig = Field(default_factory=SariMaxConfig)


class SettingsSchema(BaseModel):
    datasets: DatasetsConfig
    gold: GoldConfig = Field(default_factory=GoldConfig)
