from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.speed.event_processor import EventProcessor
from app.speed.pubsub import EventBus
from app.speed.schema import RideEvent

router = APIRouter(tags=["ingest"])


@router.post("/api/v1/ingest")
async def ingest(event: RideEvent, request: Request):
    processor: EventProcessor = request.app.state.processor
    bus: EventBus = request.app.state.event_bus

    enriched = processor.process(event)
    if enriched is None:
        return JSONResponse({"status": "rejected"}, status_code=422)

    await bus.publish(enriched)
    return {"status": "accepted", "trip_id": enriched.trip_id}
