from typing import Any, Literal

from pydantic import BaseModel, Field


class ProfilingRules(BaseModel):
    nullability: dict[str, list[str]] | None = None
    reasonableness_ranges: dict[str, dict[str, list[float]]] | None = None
    amount_formulas: dict[str, dict[str, Any]] | None = None
    max_trip_duration_minutes: int | None = None
    amount_tolerance: float | None = None


class ProfilingConfig(BaseModel):
    rules: ProfilingRules = Field(default_factory=ProfilingRules)


class StorageConfig(BaseModel):
    backend: Literal["local", "s3"] = "local"


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


class KmodesConfig(BaseModel):
    max_k: int = 8
    max_sample_per_service: int = 100_000
    n_init: int = 2
    init_method: str = "Cao"
    random_state: int = 42


class SpeedConfig(BaseModel):
    redis_url: str = "redis://localhost:6379/0"
    state_ttl_hours: int = 48
    fraud_score_threshold: float = -0.1
    block_minutes: int = 15


class ServingConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    query_cache_ttl_seconds: int = 60


class GoldConfig(BaseModel):
    mode: str = "full"
    supply_demand: SupplyDemandConfig = Field(default_factory=SupplyDemandConfig)
    abc_xyz: AbcXyzConfig = Field(default_factory=AbcXyzConfig)
    generosity: GenerosityConfig = Field(default_factory=GenerosityConfig)
    isolation_fraud: IsolationFraudConfig = Field(default_factory=IsolationFraudConfig)
    sarimax: SariMaxConfig = Field(default_factory=SariMaxConfig)
    kmodes: KmodesConfig = Field(default_factory=KmodesConfig)


class SettingsSchema(BaseModel):
    storage: StorageConfig
    datasets: DatasetsConfig
    gold: GoldConfig = Field(default_factory=GoldConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    speed: SpeedConfig = Field(default_factory=SpeedConfig)
    serving: ServingConfig = Field(default_factory=ServingConfig)
