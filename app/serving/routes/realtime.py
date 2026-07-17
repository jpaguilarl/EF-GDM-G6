from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Query, Request
from starlette.responses import StreamingResponse

from app.serving.merged_view import MergedViewReader
from app.speed.pubsub import EventBus
from app.speed.schema import EnrichedRide

router = APIRouter(prefix="/api/v1/realtime", tags=["realtime"])

VIEW_CONFIG: dict[str, dict[str, Any]] = {
    "demand-volume": {
        "mart": "mart_demand_volume",
        "time_column": "pickup_hour",
    },
    "financial-performance": {
        "mart": "mart_financial_performance",
        "time_column": "fecha_viaje",
    },
    "operational-profile": {
        "mart": "mart_operational_profile",
        "time_column": "fecha_viaje",
    },
    "supply-demand": {
        "mart": "mart_supply_demand_balance",
        "time_column": "bloque_temporal_t",
    },
    "tipping": {
        "mart": "mart_tipping_behavior",
        "time_column": "fecha_viaje",
    },
    "abc-xyz": {
        "mart": "mart_abc_xyz_zones",
        "time_column": None,
    },
}


def _build_filters(
    service_id: list[str] | None = None,
    pu_location_id: list[int] | None = None,
    bloque_horario: list[str] | None = None,
    borough: list[str] | None = None,
    categoria_generosidad: list[str] | None = None,
    location_id: list[int] | None = None,
) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if service_id is not None:
        filters["service_id"] = service_id
    if pu_location_id is not None:
        filters["pu_location_id"] = pu_location_id
    if bloque_horario is not None:
        filters["bloque_horario"] = bloque_horario
    if borough is not None:
        filters["borough"] = borough
    if categoria_generosidad is not None:
        filters["categoria_generosidad"] = categoria_generosidad
    if location_id is not None:
        filters["location_id"] = location_id
    return filters


async def _sse_format(async_gen):
    async for chunk in async_gen:
        yield f"event: {chunk['event']}\ndata: {json.dumps(chunk['data'], default=str)}\n\n"


async def event_generator(
    reader: MergedViewReader,
    bus: EventBus,
    mart: str,
    time_column: str | None,
    filters: dict[str, Any],
    service_id: list[str] | None = None,
    pu_location_id: list[int] | None = None,
    location_id: list[int] | None = None,
):
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    async def on_event(ride: EnrichedRide):
        if service_id and ride.service_id not in service_id:
            return
        if pu_location_id and ride.pu_location_id not in pu_location_id:
            return
        if location_id and ride.pu_location_id not in location_id:
            if ride.do_location_id not in location_id:
                return
        await queue.put(ride)

    bus.subscribe(on_event)
    try:
        snapshot = await reader.read_merged(
            mart, time_column, filter_cols=filters,
        )
        yield {"event": "snapshot", "data": snapshot}

        while True:
            try:
                ride = await asyncio.wait_for(queue.get(), timeout=30.0)
                row = await reader.get_realtime_row(mart, ride)
                if row is not None:
                    yield {"event": "increment", "data": row}
            except asyncio.TimeoutError:
                yield {"event": "heartbeat", "data": {}}
    finally:
        bus.unsubscribe(on_event)


async def _stream_view(
    request: Request,
    view_name: str,
    service_id: list[str] | None,
    pu_location_id: list[int] | None,
    bloque_horario: list[str] | None = None,
    borough: list[str] | None = None,
    categoria_generosidad: list[str] | None = None,
    location_id: list[int] | None = None,
):
    vc = VIEW_CONFIG[view_name]
    reader: MergedViewReader = request.app.state.merged_reader
    bus: EventBus = request.app.state.event_bus

    filters = _build_filters(
        service_id=service_id,
        pu_location_id=pu_location_id,
        bloque_horario=bloque_horario,
        borough=borough,
        categoria_generosidad=categoria_generosidad,
        location_id=location_id,
    )

    return StreamingResponse(
        _sse_format(event_generator(
            reader, bus, vc["mart"], vc["time_column"], filters,
            service_id=service_id, pu_location_id=pu_location_id, location_id=location_id,
        )),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


async def _get_merged(
    request: Request,
    view_name: str,
    service_id: list[str] | None,
    pu_location_id: list[int] | None,
    bloque_horario: list[str] | None = None,
    borough: list[str] | None = None,
    categoria_generosidad: list[str] | None = None,
    location_id: list[int] | None = None,
    limit: int = 1000,
):
    vc = VIEW_CONFIG[view_name]
    reader: MergedViewReader = request.app.state.merged_reader
    filters = _build_filters(
        service_id=service_id,
        pu_location_id=pu_location_id,
        bloque_horario=bloque_horario,
        borough=borough,
        categoria_generosidad=categoria_generosidad,
        location_id=location_id,
    )
    return await reader.read_merged(
        vc["mart"],
        vc["time_column"],
        filter_cols=filters,
        limit=limit,
    )


@router.get("/demand-volume")
async def get_demand_volume(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "demand-volume", service_id, pu_location_id, limit=limit)


@router.get("/demand-volume/stream")
async def stream_demand_volume(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "demand-volume", service_id, pu_location_id)


@router.get("/financial-performance")
async def get_financial_performance(
    service_id: list[str] | None = Query(None),
    bloque_horario: list[str] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "financial-performance", service_id, None, bloque_horario, limit=limit)


@router.get("/financial-performance/stream")
async def stream_financial_performance(
    service_id: list[str] | None = Query(None),
    bloque_horario: list[str] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "financial-performance", service_id, None, bloque_horario)


@router.get("/operational-profile")
async def get_operational_profile(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "operational-profile", service_id, pu_location_id, limit=limit)


@router.get("/operational-profile/stream")
async def stream_operational_profile(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "operational-profile", service_id, pu_location_id)


@router.get("/supply-demand")
async def get_supply_demand(
    borough: list[str] | None = Query(None),
    location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "supply-demand", None, None, borough=borough, location_id=location_id, limit=limit)


@router.get("/supply-demand/stream")
async def stream_supply_demand(
    borough: list[str] | None = Query(None),
    location_id: list[int] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "supply-demand", None, None, borough=borough, location_id=location_id)


@router.get("/tipping")
async def get_tipping(
    service_id: list[str] | None = Query(None),
    categoria_generosidad: list[str] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "tipping", service_id, None, categoria_generosidad=categoria_generosidad, limit=limit)


@router.get("/tipping/stream")
async def stream_tipping(
    service_id: list[str] | None = Query(None),
    categoria_generosidad: list[str] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "tipping", service_id, None, categoria_generosidad=categoria_generosidad)


@router.get("/abc-xyz")
async def get_abc_xyz(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    limit: int = Query(1000, le=100_000),
    request: Request = None,
):
    return await _get_merged(request, "abc-xyz", service_id, pu_location_id, limit=limit)


@router.get("/abc-xyz/stream")
async def stream_abc_xyz(
    service_id: list[str] | None = Query(None),
    pu_location_id: list[int] | None = Query(None),
    request: Request = None,
):
    return await _stream_view(request, "abc-xyz", service_id, pu_location_id)


@router.get("/fraud")
async def get_fraud(
    service_id: list[str] | None = Query(None),
    is_fraud: bool | None = Query(None),
    ratecode_id: list[int] | None = Query(None),
    limit: int = Query(100, le=10_000),
    offset: int = Query(0, ge=0),
    request: Request = None,
):
    reader: MergedViewReader = request.app.state.merged_reader
    return await reader.read_fraud(
        limit=limit, offset=offset,
        service_id=service_id, is_fraud=is_fraud, ratecode_id=ratecode_id,
    )


@router.get("/clusters")
async def get_clusters(
    service_id: list[str] | None = Query(None),
    cluster_id: list[int] | None = Query(None),
    limit: int = Query(100, le=10_000),
    offset: int = Query(0, ge=0),
    request: Request = None,
):
    reader: MergedViewReader = request.app.state.merged_reader
    return await reader.read_clusters(
        limit=limit, offset=offset,
        service_id=service_id, cluster_id=cluster_id,
    )
