from __future__ import annotations

from fastapi import APIRouter, Query, Request
import polars as pl

from app.serving.query_engine import PolarsQueryEngine

router = APIRouter(prefix="/api/v1/historic", tags=["historic"])


def _build_filters(
    service_id: list[str] | None = None,
    year: list[int] | None = None,
    month: list[int] | None = None,
    fecha_viaje: str | None = None,
    pu_location_id: list[int] | None = None,
    bloque_horario: list[str] | None = None,
    borough: list[str] | None = None,
    bloque_temporal_t: str | None = None,
    categoria_generosidad: list[str] | None = None,
) -> dict[str, list | str | int]:
    filters: dict[str, list | str | int] = {}
    if service_id is not None:
        filters["service_id"] = service_id
    if year is not None:
        filters["year"] = year
    if month is not None:
        filters["month"] = month
    if fecha_viaje is not None:
        filters["fecha_viaje"] = fecha_viaje
    if pu_location_id is not None:
        filters["pu_location_id"] = pu_location_id
    if bloque_horario is not None:
        filters["bloque_horario"] = bloque_horario
    if borough is not None:
        filters["borough"] = borough
    if bloque_temporal_t is not None:
        filters["bloque_temporal_t"] = bloque_temporal_t
    if categoria_generosidad is not None:
        filters["categoria_generosidad"] = categoria_generosidad
    return filters


def _to_response(df: pl.DataFrame, limit: int) -> list[dict]:
    return df.to_dicts()


@router.get("/demand-volume")
async def get_demand_volume(
    service_id: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
    fecha_viaje: str | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        service_id=service_id,
        year=year,
        month=month,
        fecha_viaje=fecha_viaje,
        pu_location_id=pu_location_id,
    )
    df = engine.query("mart_demand_volume", filters=filters, limit=limit)
    return _to_response(df, limit)


@router.get("/financial-performance")
async def get_financial_performance(
    service_id: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
    fecha_viaje: str | None = Query(None),
    bloque_horario: list[str] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        service_id=service_id,
        year=year,
        month=month,
        fecha_viaje=fecha_viaje,
        bloque_horario=bloque_horario,
    )
    df = engine.query("mart_financial_performance", filters=filters, limit=limit)
    return _to_response(df, limit)


@router.get("/operational-profile")
async def get_operational_profile(
    service_id: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
    fecha_viaje: str | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        service_id=service_id,
        year=year,
        month=month,
        fecha_viaje=fecha_viaje,
        pu_location_id=pu_location_id,
    )
    df = engine.query("mart_operational_profile", filters=filters, limit=limit)
    return _to_response(df, limit)


@router.get("/supply-demand-balance")
async def get_supply_demand_balance(
    borough: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
    bloque_temporal_t: str | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        borough=borough,
        year=year,
        month=month,
        bloque_temporal_t=bloque_temporal_t,
    )
    df = engine.query("mart_supply_demand_balance", filters=filters, limit=limit)
    return _to_response(df, limit)


@router.get("/abc-xyz-zones")
async def get_abc_xyz_zones(
    service_id: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        service_id=service_id,
        year=year,
        pu_location_id=pu_location_id,
    )
    df = engine.query("mart_abc_xyz_zones", filters=filters, limit=limit)
    return _to_response(df, limit)


@router.get("/tipping-behavior")
async def get_tipping_behavior(
    service_id: list[str] | None = Query(None),
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
    fecha_viaje: str | None = Query(None),
    categoria_generosidad: list[str] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    engine: PolarsQueryEngine = request.app.state.engine
    filters = _build_filters(
        service_id=service_id,
        year=year,
        month=month,
        fecha_viaje=fecha_viaje,
        categoria_generosidad=categoria_generosidad,
    )
    df = engine.query("mart_tipping_behavior", filters=filters, limit=limit)
    return _to_response(df, limit)
