# Capturas de Phoenix (prueba de carga)

Esta carpeta queda **vacía hasta que se haga una corrida real**. No se agregan
imágenes generadas ni de ejemplo.

## Cómo generar la corrida

1. `docker compose up --build` (con `OPENAI_API_KEY` en `.env`).
2. `uv run python scripts/load_test.py` → 5 POST concurrentes, aprobación automática
   del HITL y resumen con la latencia de cliente (p95) por job.
3. Abrir Phoenix en <http://localhost:6006>, proyecto `vuln-orchestrator`
   (o el valor de `PHOENIX_PROJECT_NAME`), y filtrar el rango de tiempo de esa corrida.

## Capturas a sacar (3)

| Archivo | Qué mostrar |
|---|---|
| `01_traza_job.png` | Una traza completa de un job: árbol `vuln-orchestrator-api` → supervisor → researcher → analyst → supervisor → `human_review` (+ la traza de reanudación con `synthesizer`). Que se vea el `session_id` / metadata `job_id` igual al `job_id` que imprimió el script. |
| `02_costo_por_ejecucion.png` | Vista **Sessions** (una sesión = un `job_id`, agrupa start + resume) o la tabla de trazas con la columna de **costo** y tokens de las 5 ejecuciones. Phoenix calcula el costo a partir de los tokens y el modelo. |
| `03_latencia_p95.png` | Métricas del proyecto con la **latencia p95** (y p50) de las trazas de esa corrida, con el rango de tiempo de la prueba visible. |

Opcional: copiar acá la salida de consola del script (`load_test_output.txt`) para
contrastar el p95 de cliente (incluye cola + espera de aprobación) con el p95 de Phoenix
(solo ejecución del grafo).
