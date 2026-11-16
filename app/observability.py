"""Wrapper fino sobre tracing.py (Phoenix) para el worker.

`tracing.setup_tracing()` no reintenta si Phoenix todavía no responde; en
docker compose el worker puede arrancar antes que Phoenix, así que acá se
espera hasta PHOENIX_WAIT_SECONDS antes de delegar.
"""

import os
import time

import httpx
from openinference.instrumentation import using_attributes

from tracing import DEFAULT_ENDPOINT, flush_tracing, setup_tracing


def _wait_for_phoenix(timeout: float) -> None:
    base = os.getenv("PHOENIX_COLLECTOR_ENDPOINT", DEFAULT_ENDPOINT).rstrip("/")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(base, timeout=2)
            return
        except httpx.HTTPError:
            time.sleep(1)


def init_tracing():
    """Activa Phoenix (idempotente). Devuelve el tracer provider o None."""
    if os.getenv("PHOENIX_TRACING", "true").lower() == "false":
        return None
    if not os.getenv("PHOENIX_API_KEY"):
        _wait_for_phoenix(float(os.getenv("PHOENIX_WAIT_SECONDS", "30")))
    return setup_tracing()


def flush() -> None:
    flush_tracing()


def job_attributes(job_id: str, action: str, query: str = ""):
    """Agrupa todos los spans de un job (start + resumes) en una sesión de Phoenix."""
    return using_attributes(
        session_id=job_id,
        metadata={"job_id": job_id, "action": action, "query": query},
        tags=["vuln-orchestrator", "api", action],
    )
