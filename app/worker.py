"""Worker: consume `queue:tasks` y ejecuta el grafo. Proceso APARTE de la API.

    uv run python -m app.worker

Concurrencia: un loop asyncio hace BRPOP y lanza cada job como tarea; el grafo
(nodos sync) corre en un thread con `asyncio.to_thread`, así varias corridas se
solapan. Un semáforo limita a WORKER_CONCURRENCY (default 5) jobs simultáneos y
se adquiere ANTES del BRPOP: un worker lleno no saca mensajes que no puede
correr (quedan para otra réplica). El checkpointer es RedisSaver (sync,
thread-safe vía el pool de conexiones), con thread_id = job_id.
"""

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv
from redis.asyncio import Redis

from app import jobs
from app.jobs import InvalidTransition, JobStatus
from app.observability import flush, init_tracing, job_attributes

load_dotenv()
log = logging.getLogger("worker")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))
JOB_TIMEOUT = float(os.getenv("JOB_TIMEOUT_SECONDS", "600"))


def extract_interrupt(result: dict, snapshot=None):
    """Payload del primer interrupt pendiente, o None si el grafo terminó."""
    pending = list(result.get("__interrupt__") or []) if isinstance(result, dict) else []
    if not pending and snapshot is not None:
        pending = [i for task in snapshot.tasks for i in task.interrupts]
    return pending[0].value if pending else None


def run_graph(graph, msg: dict, query: str) -> dict:
    """Corre (o reanuda) el grafo de un job. Sync: se ejecuta en un thread."""
    from langgraph.types import Command

    from app.graph import run_config
    from state import initial_state

    job_id, action = msg["job_id"], msg["action"]
    config = run_config(job_id)
    graph_input = initial_state(query) if action == "start" else Command(resume=msg["resume"])
    with job_attributes(job_id, action, query):
        result = graph.invoke(graph_input, config=config)

    payload = extract_interrupt(result, graph.get_state(config))
    if payload is not None:
        return {"interrupt": payload}
    final = (result or {}).get("final_answer")
    if not final:
        raise RuntimeError("el grafo terminó sin final_answer")
    return {"result": final}


async def handle(r: Redis, graph, raw: str) -> None:
    try:
        msg = jobs.parse_message(raw)
    except ValueError as exc:
        log.error("mensaje descartado: %s", exc)
        return
    job_id = msg["job_id"]

    job = await jobs.get_job(r, job_id)
    if job is None:
        log.error("job %s no existe, mensaje descartado", job_id)
        return
    try:
        await jobs.transition(r, job_id, JobStatus.RUNNING, expected={JobStatus.PENDING})
    except InvalidTransition as exc:
        log.warning("job %s salteado: %s", job_id, exc)
        return

    log.info("job %s %s -> RUNNING", job_id, msg["action"])
    try:
        outcome = await asyncio.wait_for(asyncio.to_thread(run_graph, graph, msg, job["query"]), JOB_TIMEOUT)
        if "interrupt" in outcome:
            await jobs.transition(r, job_id, JobStatus.AWAITING_APPROVAL,
                                  interrupt_payload=outcome["interrupt"])
            log.info("job %s -> AWAITING_APPROVAL", job_id)
        else:
            await jobs.transition(r, job_id, JobStatus.DONE, result=outcome["result"],
                                  error="", interrupt_payload="")
            log.info("job %s -> DONE", job_id)
    except asyncio.CancelledError:
        await jobs.mark_failed(r, job_id, "worker detenido durante la ejecución")
        raise
    except TimeoutError:
        await jobs.mark_failed(r, job_id, f"timeout: superó JOB_TIMEOUT_SECONDS={JOB_TIMEOUT:g}")
        log.error("job %s -> FAILED (timeout)", job_id)
    except Exception as exc:
        await jobs.mark_failed(r, job_id, f"{type(exc).__name__}: {exc}")
        log.exception("job %s -> FAILED", job_id)


async def consume(r: Redis, graph, stop: asyncio.Event, concurrency: int = CONCURRENCY) -> None:
    sem = asyncio.Semaphore(concurrency)
    running: set[asyncio.Task] = set()

    def _done(task: asyncio.Task) -> None:
        running.discard(task)
        sem.release()

    while not stop.is_set():
        await sem.acquire()
        try:
            item = await r.brpop(jobs.QUEUE_KEY, timeout=1)
        except Exception:
            sem.release()
            raise
        if item is None:
            sem.release()
            continue
        task = asyncio.create_task(handle(r, graph, item[1]))
        running.add(task)
        task.add_done_callback(_done)
    if running:
        log.info("esperando %d jobs en curso...", len(running))
        await asyncio.gather(*running, return_exceptions=True)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    init_tracing()  # antes de importar agentes/grafo, igual que main.py

    if not os.getenv("OPENAI_API_KEY"):
        sys.exit("Falta OPENAI_API_KEY (ver .env.example): el worker no puede correr el grafo.")

    from langgraph.checkpoint.redis import RedisSaver

    from app.graph import build_api_graph

    saver = RedisSaver(redis_url=REDIS_URL)
    saver.setup()  # crea los índices RediSearch del checkpointer (requiere Redis Stack / Redis 8)
    graph = build_api_graph(saver)

    r = Redis.from_url(REDIS_URL, decode_responses=True, health_check_interval=15)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("worker listo: cola=%s concurrencia=%d", jobs.QUEUE_KEY, CONCURRENCY)
    try:
        await consume(r, graph, stop)
    finally:
        await r.aclose()
        flush()
        log.info("worker detenido")


if __name__ == "__main__":
    asyncio.run(main())
