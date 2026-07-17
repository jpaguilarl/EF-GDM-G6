from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel

from app.panel import audit_reader, config_io, mart_reader, ml_reader

router = APIRouter(prefix="/api/v1/panel", tags=["panel"])


# --- Config ---

@router.get("/config")
def get_config():
    return config_io.read_config()


class ConfigUpdate(BaseModel):
    updates: dict[str, Any]


@router.put("/config")
def put_config(body: ConfigUpdate):
    try:
        config_io.write_config(body.updates)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(400, detail=str(e))


@router.get("/env")
def get_env():
    return config_io.read_env()


class EnvUpdate(BaseModel):
    updates: dict[str, str]


@router.put("/env")
def put_env(body: EnvUpdate):
    try:
        config_io.write_env(body.updates)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(400, detail=str(e))


# --- Jobs ---

class BronzeJobBody(BaseModel):
    categories: list[str]
    year: int
    month_start: int
    month_end: int


class SilverJobBody(BaseModel):
    stage: str  # quality, schema, load
    categories: Optional[list[str]] = None
    year: Optional[int] = None
    month_start: Optional[int] = None
    month_end: Optional[int] = None


class GoldJobBody(BaseModel):
    mode: str = "incremental"
    only: Optional[list[str]] = None
    categories: Optional[list[str]] = None
    year: Optional[int] = None
    month_start: Optional[int] = None
    month_end: Optional[int] = None


class GoldMlJobBody(BaseModel):
    model: str  # kmodes, isolation, sarimax


@router.post("/jobs/bronze")
async def post_bronze_job(body: BronzeJobBody, request: Request):
    jm = request.app.state.job_manager
    argv = ["--download", "--year", str(body.year),
            "--month-start", str(body.month_start), "--month-end", str(body.month_end)]
    argv.extend(["--cat"] + body.categories)
    job_id = await jm.submit("bronze", argv)
    return {"job_id": job_id}


@router.post("/jobs/silver")
async def post_silver_job(body: SilverJobBody, request: Request):
    jm = request.app.state.job_manager
    argv = ["--silver", body.stage]
    if body.categories:
        argv.extend(["--cat"] + body.categories)
    if body.year:
        argv.extend(["--year", str(body.year)])
    if body.month_start is not None and body.month_end is not None:
        argv.extend(["--month-start", str(body.month_start), "--month-end", str(body.month_end)])
    kind = f"silver-{body.stage}"
    job_id = await jm.submit(kind, argv)
    return {"job_id": job_id}


@router.post("/jobs/gold")
async def post_gold_job(body: GoldJobBody, request: Request):
    jm = request.app.state.job_manager
    argv = ["--gold", body.mode]
    if body.only:
        argv.extend(["--only", ",".join(body.only)])
    if body.categories:
        argv.extend(["--cat"] + body.categories)
    if body.year:
        argv.extend(["--year", str(body.year)])
    if body.month_start is not None and body.month_end is not None:
        argv.extend(["--month-start", str(body.month_start), "--month-end", str(body.month_end)])
    job_id = await jm.submit("gold", argv)
    return {"job_id": job_id}


@router.post("/jobs/gold-ml")
async def post_gold_ml_job(body: GoldMlJobBody, request: Request):
    jm = request.app.state.job_manager
    argv = ["--gold-ml", body.model]
    job_id = await jm.submit(f"gold-ml-{body.model}", argv)
    return {"job_id": job_id}


@router.get("/jobs")
def list_jobs(request: Request):
    return request.app.state.job_manager.list_jobs()


@router.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    job = request.app.state.job_manager.get_job(job_id)
    if not job:
        raise HTTPException(404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/logs")
async def stream_job_logs(job_id: str, request: Request):
    from sse_starlette.sse import EventSourceResponse
    jm = request.app.state.job_manager
    return EventSourceResponse(jm.stream_logs(job_id))


@router.post("/jobs/{job_id}/stop")
def stop_job(job_id: str, request: Request):
    ok = request.app.state.job_manager.stop(job_id)
    if not ok:
        raise HTTPException(404, detail="Job not found or already finished")
    return {"status": "stopped"}


# --- Audit ---

@router.get("/audit/lineage")
def get_audit_lineage(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    layer: str | None = Query(None, pattern="^(bronze|silver|gold)$"),
):
    return audit_reader.read_audit_lineage(
        limit=limit, offset=offset, layer=layer
    )


@router.get("/audit/{layer}")
def get_audit(
    layer: str = Path(..., pattern="^(bronze|silver|gold)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    category: str | None = Query(None),
    year: int | None = Query(None),
):
    return audit_reader.read_audit(layer, limit=limit, offset=offset, category=category, year=year)


# --- Audit Summary ---

@router.get("/audit/{layer}/summary")
def get_audit_summary(layer: str = Path(..., pattern="^(bronze|silver|gold)$")):
    return audit_reader.read_audit_summary(layer)


# --- Marts Summary ---

@router.get("/marts/{mart}/summary")
def get_mart_summary(
    request: Request,
    mart: str,
    year: list[int] | None = Query(None),
    month: list[int] | None = Query(None),
):
    marts_dir = getattr(request.app.state, "engine", None)
    marts_dir = marts_dir.MARTS_DIR if marts_dir else None
    return mart_reader.read_mart_summary(mart, year, month, marts_dir)


# --- ML ---

@router.get("/ml/kmodes/{service_id}")
def get_kmodes(service_id: str):
    return ml_reader.kmodes_summary(service_id)


@router.get("/ml/isolation/summary")
def get_isolation_summary():
    return ml_reader.isolation_summary()


@router.get("/ml/isolation")
def get_isolation_list():
    return ml_reader.isolation_list()


@router.get("/ml/isolation/scatter")
def get_isolation_scatter(
    ratecode: str | None = Query(None),
    limit: int = Query(500, ge=100, le=10000),
):
    return ml_reader.isolation_scatter(ratecode=ratecode, limit=limit)


@router.get("/ml/isolation/{ratecode}/scores")
def get_isolation_scores(
    ratecode: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    return ml_reader.isolation_scores(ratecode, limit=limit, offset=offset)


@router.get("/ml/sarimax/forecast")
def get_sarimax_forecast(
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    borough: str | None = Query(None),
    service_id: str | None = Query(None),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    grain: str = Query("hourly"),
):
    return ml_reader.sarimax_forecast(
        limit=limit, offset=offset, borough=borough, service_id=service_id,
        start=start_date, end=end_date, grain=grain,
    )


@router.get("/ml/sarimax/summary")
def get_sarimax_summary():
    return ml_reader.sarimax_summary()
