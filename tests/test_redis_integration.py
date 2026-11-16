"""Integración con Redis Stack real (se saltea si no hay Redis en REDIS_URL).

    docker compose up -d redis && uv run python -m pytest tests/test_redis_integration.py -q

Verifica RedisSaver.setup() (RediSearch/RedisJSON) y que el HITL pause y se
reanude desde el checkpoint en Redis, con nodos stub (sin LLM)."""

import os
import uuid

import pytest
import redis
from langgraph.types import Command

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _redis_available() -> bool:
    try:
        return redis.Redis.from_url(REDIS_URL, socket_connect_timeout=0.5).ping()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _redis_available(), reason=f"Redis no disponible en {REDIS_URL}")


def test_hitl_with_redis_checkpointer(monkeypatch):
    from langgraph.checkpoint.redis import RedisSaver

    import app.graph as api_graph
    from app import worker
    from state import initial_state

    monkeypatch.setattr(api_graph, "supervisor_node", lambda s: {
        "next_agent": "researcher" if not s.get("research_findings") else
        ("analyst" if not s.get("analysis_report") else "FINISH")})
    monkeypatch.setattr(api_graph, "research_node", lambda s: {"research_findings": "ok"})
    monkeypatch.setattr(api_graph, "analyst_node", lambda s: {"analysis_report": "reporte",
                                                              "revisions": {**s["revisions"], "analyst": 1}})
    monkeypatch.setattr(api_graph, "synthesizer_node", lambda s: {"final_answer": "FINAL"})

    saver = RedisSaver(redis_url=REDIS_URL)
    saver.setup()
    job_id = f"test-{uuid.uuid4()}"
    config = api_graph.run_config(job_id)

    graph = api_graph.build_api_graph(saver)
    result = graph.invoke(initial_state("q"), config=config)
    assert worker.extract_interrupt(result, graph.get_state(config))["analysis_report"] == "reporte"

    # Grafo nuevo (como un worker reiniciado): retoma desde el checkpoint en Redis.
    graph2 = api_graph.build_api_graph(RedisSaver(redis_url=REDIS_URL))
    result = graph2.invoke(Command(resume={"approved": True}), config=config)
    assert result["final_answer"] == "FINAL"
