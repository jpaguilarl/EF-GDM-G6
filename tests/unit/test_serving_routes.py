from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from app.serving.app import create_app
from app.serving.query_engine import PolarsQueryEngine


def _write_part(base: Path, data: dict, partition_values: dict[str, str | int],
                partition_cols: list[str] | None = None) -> None:
    if partition_cols is None:
        partition_cols = ["service_id", "year", "month"]
    part_dir = base
    for col in partition_cols:
        part_dir = part_dir / f"{col}={partition_values[col]}"
    part_dir.mkdir(parents=True, exist_ok=True)
    data_cols = {k: v for k, v in data.items() if k not in partition_cols}
    df = pl.DataFrame(data_cols)
    df.write_parquet(str(part_dir / "data.parquet"))


@pytest.fixture
def marts_dir(tmp_path: Path) -> Path:
    base = tmp_path / "marts"

    _write_part(base / "mart_demand_volume", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "pickup_hour": [14], "bloque_horario": ["Mediodía"],
        "dia_semana": [3], "is_weekend": [False],
        "pu_location_id": [1], "pu_borough": ["Manhattan"],
        "pu_zone": ["Midtown"],
        "viajes": [50], "espera_total_min": [100.0],
        "viajes_con_espera": [40], "espera_promedio_min": [2.5],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1})

    _write_part(base / "mart_financial_performance", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "bloque_horario": ["Mediodía"], "pu_location_id": [1],
        "pu_borough": ["Manhattan"], "pu_zone": ["Midtown"],
        "viajes": [50], "total_amount": [2500.0],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1})

    _write_part(base / "mart_operational_profile", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "bloque_horario": ["Mediodía"], "pu_location_id": [1],
        "pu_borough": ["Manhattan"], "pu_zone": ["Midtown"],
        "viajes": [50], "duracion_total_min": [750.0],
        "duracion_promedio_min": [15.0],
        "distancia_total_millas": [250.0],
        "distancia_promedio_millas": [5.0],
        "velocidad_promedio_mph": [20.0],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1})

    _write_part(base / "mart_supply_demand_balance", {
        "location_id": [1], "borough": ["Manhattan"],
        "zone": ["Midtown"],
        "bloque_temporal_t": ["2025-01-15T14:00:00"],
        "bloque_temporal_t_plus_1": ["2025-01-15T14:15:00"],
        "taxis_entrantes_zona_t": [30],
        "taxis_salientes_zona_t_plus_1": [20],
        "flujo_neto_oferta": [10], "deficit_severo_flag": [False],
        "year": [2025], "month": [1],
    }, {"year": 2025, "month": 1}, partition_cols=["year", "month"])

    _write_part(base / "mart_abc_xyz_zones", {
        "pu_location_id": [1], "borough": ["Manhattan"],
        "zone": ["Midtown"], "service_id": ["yellow"], "year": [2025],
        "ingresos_totales_zona": [100000.0],
        "viajes_diarios_promedio": [500.0],
        "viajes_diarios_std": [50.0],
        "coeficiente_variacion_xyz": [0.1],
        "clase_xyz": ["X"],
        "porcentaje_acumulado_ingresos": [0.25],
        "clase_abc": ["A"],
    }, {"service_id": "yellow", "year": 2025}, partition_cols=["service_id", "year"])

    _write_part(base / "mart_tipping_behavior", {
        "service_id": ["yellow"], "fecha_viaje": ["2025-01-15"],
        "pu_borough": ["Manhattan"], "do_borough": ["Brooklyn"],
        "payment_type_id": [1], "is_credit_card": [True],
        "categoria_generosidad": ["Estandar"],
        "viajes": [30], "viajes_con_propina": [25],
        "propina_total": [150.0],
        "porcentaje_propina_promedio": [15.0],
        "porcentaje_propina_ponderado": [14.5],
        "propina_por_milla": [1.5],
        "year": [2025], "month": [1],
    }, {"service_id": "yellow", "year": 2025, "month": 1})

    return base


@pytest.fixture
def client(marts_dir: Path):
    app = create_app()
    app.state.engine = PolarsQueryEngine(marts_dir)
    with TestClient(app) as c:
        yield c


class TestHealth:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["layer"] == "serving"


class TestDemandVolume:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/demand-volume")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["service_id"] == "yellow"

    def test_filter_service_id(self, client):
        resp = client.get("/api/v1/historic/demand-volume?service_id=yellow")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_filter_multi_service_id(self, client):
        _write_part(client.app.state.engine.MARTS_DIR / "mart_demand_volume", {
            "service_id": ["green"], "fecha_viaje": ["2025-01-15"],
            "pickup_hour": [10], "bloque_horario": ["Mediodía"],
            "dia_semana": [3], "is_weekend": [False],
            "pu_location_id": [2], "pu_borough": ["Queens"],
            "pu_zone": ["Astoria"],
            "viajes": [30], "espera_total_min": [60.0],
            "viajes_con_espera": [20], "espera_promedio_min": [1.5],
            "year": [2025], "month": [1],
        }, {"service_id": "green", "year": 2025, "month": 1})
        resp = client.get("/api/v1/historic/demand-volume?service_id=yellow&service_id=green")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_limit(self, client):
        resp = client.get("/api/v1/historic/demand-volume?limit=1")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_limit_capped(self, client):
        resp = client.get("/api/v1/historic/demand-volume?limit=200000")
        assert resp.status_code == 422


class TestFinancialPerformance:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/financial-performance")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1

    def test_filter_bloque_horario(self, client):
        resp = client.get("/api/v1/historic/financial-performance?bloque_horario=Mediod%C3%ADa")
        assert resp.status_code == 200


class TestOperationalProfile:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/operational-profile")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1


class TestSupplyDemandBalance:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/supply-demand-balance")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    def test_filter_borough(self, client):
        resp = client.get("/api/v1/historic/supply-demand-balance?borough=Manhattan")
        assert resp.status_code == 200


class TestAbcXyzZones:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/abc-xyz-zones")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    def test_filter_year(self, client):
        resp = client.get("/api/v1/historic/abc-xyz-zones?year=2025")
        assert resp.status_code == 200


class TestTippingBehavior:
    def test_list(self, client):
        resp = client.get("/api/v1/historic/tipping-behavior")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    def test_filter_categoria(self, client):
        resp = client.get("/api/v1/historic/tipping-behavior?categoria_generosidad=Estandar")
        assert resp.status_code == 200
