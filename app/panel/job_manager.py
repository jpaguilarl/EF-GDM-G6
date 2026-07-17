import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncGenerator
import uuid


@dataclass
class Job:
    id: str
    kind: str
    status: str
    pid: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    exit_code: int | None = None
    logs: deque = field(default_factory=lambda: deque(maxlen=10000))


class JobManager:
    def __init__(self, engine: object | None = None):
        self._jobs: dict[str, Job] = {}
        self._stream_queues: dict[str, asyncio.Queue] = {}
        self._engine = engine

    async def submit(self, kind: str, argv: list[str]) -> str:
        job_id = str(uuid.uuid4())
        job = Job(id=job_id, kind=kind, status="running", started_at=datetime.now())
        self._jobs[job_id] = job
        self._stream_queues[job_id] = asyncio.Queue()

        proc = await asyncio.create_subprocess_exec(
            "uv", "run", "main.py", *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        job.pid = proc.pid

        async def _reader():
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                job.logs.append(text)
                q = self._stream_queues.get(job_id)
                if q:
                    await q.put(text)
            exit_code = await proc.wait()
            job.status = "completed" if exit_code == 0 else "failed"
            job.exit_code = exit_code
            job.ended_at = datetime.now()
            if self._engine and exit_code == 0:
                self._engine.invalidate_all()
            q = self._stream_queues.get(job_id)
            if q:
                await q.put(None)

        asyncio.create_task(_reader())
        return job_id

    def list_jobs(self) -> list[dict]:
        return [
            {
                "id": j.id,
                "kind": j.kind,
                "status": j.status,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "ended_at": j.ended_at.isoformat() if j.ended_at else None,
                "exit_code": j.exit_code,
            }
            for j in sorted(
                self._jobs.values(),
                key=lambda j: j.started_at or datetime.min,
                reverse=True,
            )
        ]

    def get_job(self, job_id: str) -> dict | None:
        j = self._jobs.get(job_id)
        if not j:
            return None
        return {
            "id": j.id,
            "kind": j.kind,
            "status": j.status,
            "pid": j.pid,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "ended_at": j.ended_at.isoformat() if j.ended_at else None,
            "exit_code": j.exit_code,
            "logs_tail": list(j.logs)[-200:],
        }

    async def stream_logs(self, job_id: str) -> AsyncGenerator[str, None]:
        q = self._stream_queues.get(job_id)
        if not q:
            return
        while True:
            line = await q.get()
            if line is None:
                yield "event: done\ndata: \n\n"
                break
            yield f"data: {line}\n\n"

    def stop(self, job_id: str) -> bool:
        j = self._jobs.get(job_id)
        if not j or j.pid is None:
            return False
        try:
            import os
            import signal
            os.kill(j.pid, signal.SIGTERM)
            j.status = "stopped"
            j.ended_at = datetime.now()
            return True
        except ProcessLookupError:
            return False
