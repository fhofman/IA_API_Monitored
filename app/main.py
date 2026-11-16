"""API async del orquestador. NO importa agentes ni el grafo ni llama al LLM:
solo escribe/lee jobs en Redis y encola mensajes para el worker.

    uv run uvicorn app.main:app --reload
"""

import os
from contextlib import asynccontextmanager
from typing import Callable

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app import jobs
from app.jobs import InvalidTransition, JobStatus

load_dotenv()


class TaskRequest(BaseModel):
    query: str = Field(min_length=1, max_length=4000, description="Dependencias a analizar")


class ApproveRequest(BaseModel):
    approved: bool
    feedback: str | None = Field(default=None, max_length=4000)


class TaskAccepted(BaseModel):
    job_id: str
    status: JobStatus


def default_redis() -> Redis:
    return Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                          decode_responses=True, health_check_interval=15)


def create_app(redis_factory: Callable[[], Redis] = default_redis) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.redis = redis_factory()
        try:
            yield
        finally:
            await app.state.redis.aclose()

    app = FastAPI(title="Vuln Orchestrator API", version="0.1.0", lifespan=lifespan)

    def redis_of(request: Request) -> Redis:
        return request.app.state.redis

    @app.post("/tasks", status_code=202, response_model=TaskAccepted)
    async def create_task(body: TaskRequest, request: Request):
        job_id = await jobs.create_job(redis_of(request), body.query.strip())
        return TaskAccepted(job_id=job_id, status=JobStatus.PENDING)

    @app.get("/tasks/{job_id}")
    async def get_task(job_id: str, request: Request):
        job = await jobs.get_job(redis_of(request), job_id)
        if job is None:
            raise HTTPException(404, f"job {job_id} no existe")
        status = job["status"]
        out = {k: job[k] for k in ("job_id", "status", "query", "created_at", "updated_at")}
        if status == JobStatus.DONE:
            out["result"] = job["result"]
        elif status == JobStatus.AWAITING_APPROVAL:
            out["interrupt_payload"] = job["interrupt_payload"]
        elif status == JobStatus.FAILED:
            out["error"] = job["error"]
        return out

    @app.post("/tasks/{job_id}/approve", status_code=202, response_model=TaskAccepted)
    async def approve_task(job_id: str, body: ApproveRequest, request: Request):
        r = redis_of(request)
        if await jobs.get_job(r, job_id) is None:
            raise HTTPException(404, f"job {job_id} no existe")
        try:
            # Cambio de estado + encolado atómicos: un doble approve da 409.
            await jobs.transition(
                r, job_id, JobStatus.PENDING,
                expected={JobStatus.AWAITING_APPROVAL},
                enqueue=jobs.resume_message(job_id, body.approved, body.feedback),
                interrupt_payload="",
            )
        except InvalidTransition as exc:
            raise HTTPException(409, f"el job no está esperando aprobación (estado: {exc.current})") from exc
        return TaskAccepted(job_id=job_id, status=JobStatus.PENDING)

    @app.get("/health")
    async def health(request: Request):
        try:
            await redis_of(request).ping()
        except Exception as exc:
            raise HTTPException(503, f"redis no disponible: {exc}") from exc
        return {"status": "ok", "redis": "ok"}

    return app


app = create_app()
