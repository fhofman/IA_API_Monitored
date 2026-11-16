"""Transiciones de estado, mensajes de cola y serialización (Redis fake)."""

import asyncio
import json

import fakeredis
import pytest

from app import jobs
from app.jobs import InvalidTransition, JobStatus


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize("current, target, ok", [
    ("PENDING", "RUNNING", True),
    ("RUNNING", "AWAITING_APPROVAL", True),
    ("RUNNING", "DONE", True),
    ("RUNNING", "FAILED", True),
    ("AWAITING_APPROVAL", "PENDING", True),
    ("AWAITING_APPROVAL", "RUNNING", False),
    ("PENDING", "DONE", False),
    ("DONE", "FAILED", False),
    ("FAILED", "RUNNING", False),
    (None, "RUNNING", False),
    ("RUNNING", "BOGUS", False),
])
def test_can_transition(current, target, ok):
    assert jobs.can_transition(current, target) is ok


def test_messages_roundtrip():
    assert jobs.parse_message(jobs.start_message("j1")) == {"job_id": "j1", "action": "start"}
    msg = jobs.parse_message(jobs.resume_message("j1", False, "más detalle"))
    assert msg["resume"] == {"approved": False, "feedback": "más detalle"}


@pytest.mark.parametrize("raw", ['{"action": "start"}', '{"job_id": "x", "action": "boom"}',
                                 '{"job_id": "x", "action": "resume"}', "[]"])
def test_parse_message_invalid(raw):
    with pytest.raises(ValueError):
        jobs.parse_message(raw)


def test_serialize_and_deserialize():
    fields = jobs.serialize_fields({"status": JobStatus.DONE, "interrupt_payload": {"a": [1]}, "error": None})
    assert fields == {"status": "DONE", "interrupt_payload": '{"a": [1]}', "error": ""}
    job = jobs.deserialize_job("j", {b"status": b"DONE", b"interrupt_payload": b'{"a": [1]}', b"error": b""})
    assert job["status"] == "DONE" and job["interrupt_payload"] == {"a": [1]} and job["error"] is None


def test_create_transition_and_fail():
    async def scenario():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        job_id = await jobs.create_job(r, "lodash 4.17.15")
        job = await jobs.get_job(r, job_id)
        assert job["status"] == "PENDING" and job["created_at"]
        assert json.loads(await r.rpop(jobs.QUEUE_KEY)) == {"job_id": job_id, "action": "start"}

        await jobs.transition(r, job_id, JobStatus.RUNNING, expected={JobStatus.PENDING})
        with pytest.raises(InvalidTransition):
            await jobs.transition(r, job_id, JobStatus.RUNNING, expected={JobStatus.PENDING})

        await jobs.mark_failed(r, job_id, "ValueError: boom")
        job = await jobs.get_job(r, job_id)
        assert job["status"] == "FAILED" and job["error"] == "ValueError: boom"
        await jobs.mark_failed(r, job_id, "otra")  # terminal: no cambia ni levanta
        assert (await jobs.get_job(r, job_id))["error"] == "ValueError: boom"

    run(scenario())
