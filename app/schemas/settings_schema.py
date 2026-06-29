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


class GoldConfig(BaseModel):
    mode: str = "full"
    supply_demand: SupplyDemandConfig = Field(default_factory=SupplyDemandConfig)
    abc_xyz: AbcXyzConfig = Field(default_factory=AbcXyzConfig)
    generosity: GenerosityConfig = Field(default_factory=GenerosityConfig)


class SettingsSchema(BaseModel):
    datasets: DatasetsConfig
    gold: GoldConfig = Field(default_factory=GoldConfig)
