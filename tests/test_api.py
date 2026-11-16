"""API con Redis fake: no hay worker ni grafo, solo se verifica el contrato HTTP."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient

from app import jobs
from app.jobs import JobStatus
from app.main import create_app


@pytest.fixture
def ctx():
    server = fakeredis.FakeServer()
    app = create_app(lambda: fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
    sync_r = fakeredis.FakeRedis(server=server, decode_responses=True)
    with TestClient(app) as client:
        yield client, sync_r


def set_status(r, job_id, status, **fields):
    r.hset(jobs.job_key(job_id), mapping={"status": status, **fields})


def test_api_imports_without_openai_key_or_redis():
    """Importar la API no construye agentes ni el grafo ni conecta a Redis."""
    code = ("import sys, app.main; "
            "bad = [m for m in ('app.graph', 'graph', 'agents', 'langchain_openai') if m in sys.modules]; "
            "assert not bad, bad")
    env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
    env["REDIS_URL"] = "redis://127.0.0.1:1/0"  # nada escucha ahí
    proc = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True,
                          cwd=Path(__file__).resolve().parents[1])
    assert proc.returncode == 0, proc.stderr


def test_post_enqueues_and_returns_202(ctx):
    client, r = ctx
    resp = client.post("/tasks", json={"query": "lodash 4.17.15 (npm)"})
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "PENDING" and body["job_id"]
    assert json.loads(r.rpop(jobs.QUEUE_KEY)) == {"job_id": body["job_id"], "action": "start"}

    got = client.get(f"/tasks/{body['job_id']}").json()
    assert got["status"] == "PENDING" and got["query"] == "lodash 4.17.15 (npm)"


def test_post_validates_query(ctx):
    client, _ = ctx
    assert client.post("/tasks", json={"query": ""}).status_code == 422
    assert client.post("/tasks", json={}).status_code == 422


def test_get_404(ctx):
    client, _ = ctx
    assert client.get("/tasks/nope").status_code == 404
    assert client.post("/tasks/nope/approve", json={"approved": True}).status_code == 404


def test_get_shows_result_error_and_payload(ctx):
    client, r = ctx
    set_status(r, "d", "DONE", result="# Reporte", query="q")
    set_status(r, "f", "FAILED", error="RuntimeError: x", query="q")
    set_status(r, "a", "AWAITING_APPROVAL", interrupt_payload=json.dumps({"analysis_report": "r"}), query="q")
    assert client.get("/tasks/d").json()["result"] == "# Reporte"
    assert client.get("/tasks/f").json()["error"] == "RuntimeError: x"
    assert client.get("/tasks/a").json()["interrupt_payload"] == {"analysis_report": "r"}


def test_approve_requires_awaiting_state(ctx):
    client, r = ctx
    job_id = client.post("/tasks", json={"query": "q"}).json()["job_id"]
    r.delete(jobs.QUEUE_KEY)
    assert client.post(f"/tasks/{job_id}/approve", json={"approved": True}).status_code == 409

    set_status(r, job_id, JobStatus.AWAITING_APPROVAL, interrupt_payload="{}")
    resp = client.post(f"/tasks/{job_id}/approve", json={"approved": False, "feedback": "más detalle"})
    assert resp.status_code == 202 and resp.json()["status"] == "PENDING"
    assert json.loads(r.rpop(jobs.QUEUE_KEY)) == {
        "job_id": job_id, "action": "resume", "resume": {"approved": False, "feedback": "más detalle"}}
    # Un segundo approve ya no corresponde.
    assert client.post(f"/tasks/{job_id}/approve", json={"approved": True}).status_code == 409


def test_health(ctx):
    client, _ = ctx
    assert client.get("/health").json() == {"status": "ok", "redis": "ok"}


def test_five_concurrent_posts_are_enqueued():
    """5 POST concurrentes responden 202 y quedan 5 mensajes en la cola."""
    app = create_app()
    app.state.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

    async def burst():
        transport = httpx.ASGITransport(app=app)  # sin lifespan: usamos el Redis fake de arriba
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
            responses = await asyncio.gather(*[ac.post("/tasks", json={"query": f"q{i}"}) for i in range(5)])
            return responses, await app.state.redis.llen(jobs.QUEUE_KEY)

    responses, queued = asyncio.run(burst())
    assert [x.status_code for x in responses] == [202] * 5
    assert len({x.json()["job_id"] for x in responses}) == 5
    assert queued == 5
