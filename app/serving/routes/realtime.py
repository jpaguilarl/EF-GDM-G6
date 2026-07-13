from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/v1/realtime", tags=["realtime"])


@router.get("/demand-volume/stream")
async def stream_demand_volume(request: Request):
    raise HTTPException(501, "Real-time layer not yet implemented")
