from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.post("/reload-models")
async def reload_models(request: Request):
    model_loader = request.app.state.model_loader
    if model_loader is None:
        return JSONResponse({"status": "error", "message": "ModelLoader not initialized"}, status_code=500)
    model_loader.load()
    return {"status": "ok", "message": "Models reloaded"}
