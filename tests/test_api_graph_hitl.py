"""HITL de punta a punta con LangGraph real (interrupt + Command) y nodos stub.

Se reemplazan los nodos de los agentes por funciones deterministas: no hay LLM
ni red. El checkpointer es en memoria (el worker usa RedisSaver)."""

import asyncio

import fakeredis
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

import app.graph as api_graph
from app import jobs, worker
from app.jobs import JobStatus
from state import initial_state


@pytest.fixture
def stub_graph(monkeypatch):
    calls = []

    def supervisor(state):
        calls.append("supervisor")
        if not state.get("research_findings"):
            nxt = "researcher"
        elif not state.get("analysis_report"):
            nxt = "analyst"
        else:
            nxt = "FINISH"
        return {"next_agent": nxt, "step_count": state.get("step_count", 0) + 1}

    def researcher(state):
        calls.append("researcher")
        return {"research_findings": "lodash: 2 vulns", "revisions": {**state["revisions"], "researcher": 1}}

    def analyst(state):
        calls.append("analyst")
        n = state["revisions"]["analyst"] + 1
        return {"analysis_report": f"reporte v{n} ({state.get('current_instruction', '')})",
                "revisions": {**state["revisions"], "analyst": n}}

    def synthesizer(state):
        calls.append("synthesizer")
        return {"final_answer": f"FINAL: {state['analysis_report']}", "task_completed": True}

    for name, fn in [("supervisor_node", supervisor), ("research_node", researcher),
                     ("analyst_node", analyst), ("synthesizer_node", synthesizer)]:
        monkeypatch.setattr(api_graph, name, fn)
    return api_graph.build_api_graph(InMemorySaver()), calls


def test_interrupt_pauses_before_synthesizer_and_resumes(stub_graph):
    graph, calls = stub_graph
    config = api_graph.run_config("job-1")

    result = graph.invoke(initial_state("lodash 4.17.15"), config=config)
    payload = worker.extract_interrupt(result, graph.get_state(config))
    assert payload["type"] == "approval_request"
    assert payload["analysis_report"].startswith("reporte v1")
    assert "synthesizer" not in calls
    assert graph.get_state(config).next == ("human_review",)

    # Rechazo: vuelve al analista con el feedback y vuelve a pausar.
    result = graph.invoke(Command(resume={"approved": False, "feedback": "agregá CVEs"}), config=config)
    payload = worker.extract_interrupt(result, graph.get_state(config))
    assert payload is not None and "agregá CVEs" in payload["analysis_report"]
    assert calls.count("analyst") == 2 and "synthesizer" not in calls

    # Aprobación: corre la síntesis y termina.
    result = graph.invoke(Command(resume={"approved": True}), config=config)
    assert worker.extract_interrupt(result, graph.get_state(config)) is None
    assert result["final_answer"].startswith("FINAL: reporte v2")
    assert graph.get_state(config).next == ()


def test_cli_graph_has_no_human_review():
    from graph import graph as cli_graph
    assert "human_review" not in cli_graph.get_graph().nodes
    assert cli_graph.checkpointer is None


class FakeGraph:
    def __init__(self, exc=None, interrupt=None, final="# ok"):
        self.exc, self.interrupt, self.final = exc, interrupt, final

    def invoke(self, graph_input, config):
        if self.exc:
            raise self.exc
        if self.interrupt:
            return {"__interrupt__": [type("I", (), {"value": self.interrupt})()]}
        return {"final_answer": self.final}

    def get_state(self, config):
        return type("S", (), {"tasks": ()})()


@pytest.mark.parametrize("graph, status, field, expected", [
    (FakeGraph(exc=RuntimeError("OSV caído")), JobStatus.FAILED, "error", "RuntimeError: OSV caído"),
    (FakeGraph(interrupt={"analysis_report": "r"}), JobStatus.AWAITING_APPROVAL, "interrupt_payload", {"analysis_report": "r"}),
    (FakeGraph(final="# Reporte"), JobStatus.DONE, "result", "# Reporte"),
    (FakeGraph(final=""), JobStatus.FAILED, "error", "RuntimeError: el grafo terminó sin final_answer"),
])
def test_worker_handle_sets_final_status(graph, status, field, expected):
    async def scenario():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        job_id = await jobs.create_job(r, "lodash 4.17.15")
        raw = await r.rpop(jobs.QUEUE_KEY)
        await worker.handle(r, graph, raw)
        return await jobs.get_job(r, job_id)

    job = asyncio.run(scenario())
    assert job["status"] == status
    assert job[field] == expected


def test_worker_consumes_jobs_concurrently():
    """5 jobs que tardan 0.8 s cada uno terminan en ~1 s, no en 4 s (serie)."""
    import time

    class SlowGraph(FakeGraph):
        def invoke(self, graph_input, config):
            time.sleep(0.8)
            return {"final_answer": "# ok"}

    async def scenario():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        ids = [await jobs.create_job(r, f"q{i}") for i in range(5)]
        stop = asyncio.Event()

        async def stop_when_done():
            while True:
                states = [(await jobs.get_job(r, i))["status"] for i in ids]
                if all(s == "DONE" for s in states):
                    stop.set()
                    return
                await asyncio.sleep(0.05)

        t0 = time.monotonic()
        await asyncio.wait_for(asyncio.gather(worker.consume(r, SlowGraph(), stop, concurrency=5),
                                              stop_when_done()), timeout=10)
        return time.monotonic() - t0

    assert asyncio.run(scenario()) < 3.0
