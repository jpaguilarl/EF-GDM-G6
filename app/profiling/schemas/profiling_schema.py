from datetime import datetime
from typing import Any

from pydantic import BaseModel


class Metric(BaseModel):
    name: str
    value: float | int | str
    passed: bool | None = None
    detail: dict[str, Any] = {}


class DimensionResult(BaseModel):
    dimension: str
    score: float
    passed: bool
    metrics: list[Metric] = []
    failures_sample: list[dict[str, Any]] = []


class DatasetMeta(BaseModel):
    name: str
    category: str
    year: int
    month: int
    rowcount: int
    columns: list[str]
    time_span: tuple[str, str] | None = None
    file_path: str
    generated_at: str


class ProfilingReport(BaseModel):
    meta: DatasetMeta
    dimensions: list[DimensionResult]
    overall_score: float
