"""Modelo de jobs en Redis: claves, serialización y transiciones de estado.

Hash `job:{id}`: status, query, created_at, updated_at, error, result,
interrupt_payload (JSON). Cola `queue:tasks` (LPUSH en la API, BRPOP en el worker).
Sin LLM ni grafo: lo usan tanto la API como el worker.
"""

import json
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

QUEUE_KEY = "queue:tasks"
JOB_PREFIX = "job:"


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    FAILED = "FAILED"
    DONE = "DONE"


TERMINAL = {JobStatus.DONE, JobStatus.FAILED}

# PENDING también se usa para "reanudación encolada" tras un approve.
ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.FAILED},
    JobStatus.RUNNING: {JobStatus.AWAITING_APPROVAL, JobStatus.DONE, JobStatus.FAILED},
    JobStatus.AWAITING_APPROVAL: {JobStatus.PENDING, JobStatus.FAILED},
    JobStatus.DONE: set(),
    JobStatus.FAILED: set(),
}


class InvalidTransition(Exception):
    def __init__(self, job_id: str, current: str | None, target: str):
        super().__init__(f"job {job_id}: transición inválida {current} -> {target}")
        self.current = current
        self.target = target


def job_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_job_id() -> str:
    return str(uuid.uuid4())


def can_transition(current: str | None, target: str) -> bool:
    try:
        return JobStatus(target) in ALLOWED_TRANSITIONS[JobStatus(current)]
    except (ValueError, KeyError):
        return False


# --- Mensajes de la cola --------------------------------------------------------

def start_message(job_id: str) -> str:
    return json.dumps({"job_id": job_id, "action": "start"})


def resume_message(job_id: str, approved: bool, feedback: str | None) -> str:
    return json.dumps({"job_id": job_id, "action": "resume",
                       "resume": {"approved": approved, "feedback": feedback}})


def parse_message(raw: str | bytes) -> dict:
    msg = json.loads(raw)
    if not isinstance(msg, dict) or not msg.get("job_id"):
        raise ValueError(f"mensaje sin job_id: {raw!r}")
    if msg.get("action") not in ("start", "resume"):
        raise ValueError(f"acción desconocida: {msg.get('action')!r}")
    if msg["action"] == "resume" and not isinstance(msg.get("resume"), dict):
        raise ValueError("resume sin payload")
    return msg


# --- Serialización del hash -------------------------------------------------------

def serialize_fields(fields: dict[str, Any]) -> dict[str, str]:
    """Redis guarda strings: dict/list van como JSON, None como ''."""
    out = {}
    for k, v in fields.items():
        if v is None:
            out[k] = ""
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False, default=str)
        else:
            out[k] = str(v)
    return out


def deserialize_job(job_id: str, raw: dict) -> dict:
    """Hash de Redis -> dict para la API (interrupt_payload vuelve a ser JSON)."""
    data = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in raw.items()}
    payload = data.get("interrupt_payload") or ""
    try:
        interrupt_payload = json.loads(payload) if payload else None
    except json.JSONDecodeError:
        interrupt_payload = payload
    return {
        "job_id": job_id,
        "status": data.get("status"),
        "query": data.get("query", ""),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "error": data.get("error") or None,
        "result": data.get("result") or None,
        "interrupt_payload": interrupt_payload,
    }


# --- Operaciones sobre Redis ------------------------------------------------------

async def create_job(r: Redis, query: str, job_id: str | None = None) -> str:
    """Crea el hash en PENDING y encola el start en una sola transacción."""
    job_id = job_id or new_job_id()
    ts = now_iso()
    fields = serialize_fields({
        "status": JobStatus.PENDING, "query": query, "created_at": ts, "updated_at": ts,
        "error": "", "result": "", "interrupt_payload": "",
    })
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(job_key(job_id), mapping=fields)
        pipe.lpush(QUEUE_KEY, start_message(job_id))
        await pipe.execute()
    return job_id


async def get_job(r: Redis, job_id: str) -> dict | None:
    raw = await r.hgetall(job_key(job_id))
    return deserialize_job(job_id, raw) if raw else None


async def transition(r: Redis, job_id: str, target: JobStatus, *,
                     expected: set[JobStatus] | None = None,
                     enqueue: str | None = None, **fields: Any) -> None:
    """Cambia el estado con chequeo optimista (WATCH): si otro proceso lo movió
    en el medio, o la transición no es válida, levanta InvalidTransition.
    `enqueue` permite encolar un mensaje en la misma transacción (approve)."""
    key = job_key(job_id)
    async with r.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(key)
            current = await pipe.hget(key, "status")
            current = current.decode() if isinstance(current, bytes) else current
            allowed = can_transition(current, target) and (expected is None or current in expected)
            if not allowed:
                raise InvalidTransition(job_id, current, target)
            pipe.multi()
            pipe.hset(key, mapping=serialize_fields({**fields, "status": target, "updated_at": now_iso()}))
            if enqueue:
                pipe.lpush(QUEUE_KEY, enqueue)
            await pipe.execute()
        except WatchError as exc:
            raise InvalidTransition(job_id, None, target) from exc


async def mark_failed(r: Redis, job_id: str, error: str) -> None:
    """FAILED desde cualquier estado no terminal. No levanta si ya era terminal."""
    for _ in range(3):
        try:
            await transition(r, job_id, JobStatus.FAILED, error=error[:4000])
            return
        except InvalidTransition as exc:
            if exc.current is not None:  # ya terminal o inexistente: nada que hacer
                return
