"""Prueba de carga: 5 POST /tasks concurrentes + polling hasta DONE/FAILED.

    uv run python scripts/load_test.py                     # aprueba solo los HITL
    uv run python scripts/load_test.py --no-auto-approve   # los deja en AWAITING_APPROVAL
    API_BASE_URL=http://host:8000 uv run python scripts/load_test.py --timeout 900

Imprime por job: estado final, latencia punta a punta (POST -> estado final,
incluye el tiempo en cola y las aprobaciones) y el p95 de esas latencias.
El costo por ejecución y el p95 "oficial" se leen en Phoenix (ver screenshots/README.md).
"""

import argparse
import asyncio
import math
import os
import time

import httpx

QUERIES = [
    "Tengo lodash 4.17.15 y express 4.17.1 (npm). ¿Qué vulnerabilidades tienen y qué arreglo primero?",
    "Analizá django 3.2.0 y requests 2.25.0 (PyPI).",
    "¿Es seguro log4j-core 2.14.1 (Maven, org.apache.logging.log4j:log4j-core)?",
    "Revisá guzzlehttp/guzzle 7.4.0 (Packagist) y symfony/http-kernel 5.4.0.",
    "Tengo axios 0.21.1 (npm) y pyyaml 5.3 (PyPI): priorizá la remediación.",
]


def p95(values: list[float]) -> float:
    """Percentil 95 por rango más cercano (con 5 muestras es el máximo)."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


async def run_job(client: httpx.AsyncClient, query: str, args) -> dict:
    t0 = time.perf_counter()
    resp = await client.post("/tasks", json={"query": query})
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    approvals = 0
    print(f"→ {job_id} encolado ({(time.perf_counter() - t0) * 1000:.0f} ms)")

    deadline = t0 + args.timeout
    status = "PENDING"
    body: dict = {}
    while time.perf_counter() < deadline:
        await asyncio.sleep(args.interval)
        body = (await client.get(f"/tasks/{job_id}")).json()
        status = body["status"]
        if status in ("DONE", "FAILED"):
            break
        if status == "AWAITING_APPROVAL":
            if not args.auto_approve:
                break
            r = await client.post(f"/tasks/{job_id}/approve", json={"approved": True, "feedback": "auto-approve (load test)"})
            if r.status_code == 202:
                approvals += 1
                print(f"  ✔ {job_id} aprobado automáticamente (#{approvals})")
    else:
        status = f"TIMEOUT({status})"

    return {"job_id": job_id, "status": status, "latency": time.perf_counter() - t0,
            "approvals": approvals, "error": body.get("error"), "query": query}


async def main(args) -> None:
    async with httpx.AsyncClient(base_url=args.base_url, timeout=30) as client:
        (await client.get("/health")).raise_for_status()
        t0 = time.perf_counter()
        results = await asyncio.gather(*[run_job(client, q, args) for q in QUERIES[: args.n]])
        wall = time.perf_counter() - t0

    print("\n" + "=" * 90)
    print(f"{'job_id':36}  {'estado':20} {'latencia':>9}  aprob.")
    for r in results:
        print(f"{r['job_id']:36}  {r['status']:20} {r['latency']:8.1f}s  {r['approvals']}")
        if r["error"]:
            print(f"    error: {r['error'][:200]}")
    finished = [r["latency"] for r in results if r["status"] == "DONE"]
    all_lat = [r["latency"] for r in results]
    print("=" * 90)
    print(f"DONE: {len(finished)}/{len(results)}  |  tiempo total (pared): {wall:.1f}s")
    print(f"latencia cliente  p50={sorted(all_lat)[len(all_lat) // 2]:.1f}s  "
          f"p95={p95(all_lat):.1f}s  max={max(all_lat):.1f}s")
    print("Costo por ejecución y p95 de la plataforma: ver Phoenix (http://localhost:6006).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("-n", type=int, default=5, help="cantidad de peticiones concurrentes (máx. 5)")
    parser.add_argument("--timeout", type=float, default=600, help="segundos máximos por job")
    parser.add_argument("--interval", type=float, default=2, help="segundos entre polls")
    parser.add_argument("--no-auto-approve", dest="auto_approve", action="store_false",
                        help="no aprobar: deja los jobs en AWAITING_APPROVAL")
    asyncio.run(main(parser.parse_args()))
