"""Human-in-the-loop: payload del interrupt, parseo de la decisión y ruteo.

Todo es puro (sin LLM, sin Redis): el nodo `human_review` de app/graph.py solo
combina estas funciones con `interrupt()`. Así se testea sin infraestructura.
"""

from dataclasses import dataclass
from typing import Any, Literal

from langchain_core.messages import HumanMessage

from state import MAX_RUNS_PER_AGENT, make_contribution

ReviewTarget = Literal["synthesizer", "analyst"]

REVIEWER = "human_review"
DEFAULT_REJECT_FEEDBACK = "El revisor humano rechazó el reporte sin dar detalles: revisalo y mejoralo."


@dataclass(frozen=True)
class ReviewDecision:
    approved: bool
    feedback: str | None = None


def build_interrupt_payload(state: dict) -> dict:
    """Lo que ve el humano (y GET /tasks/{id}) mientras el job está pausado."""
    runs = state.get("revisions", {}) or {}
    return {
        "type": "approval_request",
        "question": "¿Aprobás el análisis para generar el reporte final?",
        "user_query": state.get("user_query", ""),
        "analysis_report": state.get("analysis_report", ""),
        "research_findings": state.get("research_findings", ""),
        "pending_issues": list(state.get("pending_issues", []) or []),
        "supervisor_feedback": state.get("supervisor_feedback", ""),
        "step_count": state.get("step_count", 0),
        "revisions": dict(runs),
        "can_request_changes": can_revise(state),
    }


def can_revise(state: dict) -> bool:
    """Un rechazo solo vuelve al analista si hay reporte y le quedan ejecuciones."""
    runs = state.get("revisions", {}) or {}
    return bool(state.get("analysis_report")) and runs.get("analyst", 0) < MAX_RUNS_PER_AGENT


def parse_decision(raw: Any) -> ReviewDecision:
    """Normaliza lo que llega por Command(resume=...). Acepta dict o bool."""
    if isinstance(raw, ReviewDecision):
        return raw
    if isinstance(raw, bool):
        return ReviewDecision(approved=raw)
    if not isinstance(raw, dict) or not isinstance(raw.get("approved"), bool):
        raise ValueError(f"Decisión de revisión inválida: {raw!r} (se espera {{'approved': bool, 'feedback'?: str}})")
    feedback = raw.get("feedback")
    if feedback is not None and not isinstance(feedback, str):
        raise ValueError("feedback debe ser string o null")
    feedback = feedback.strip() if feedback else None
    return ReviewDecision(approved=raw["approved"], feedback=feedback or None)


def resolve_review(state: dict, decision: ReviewDecision) -> tuple[ReviewTarget, dict]:
    """Devuelve (próximo nodo, update del estado) según la decisión humana.

    - Aprobado -> synthesizer.
    - Rechazado con revisiones disponibles -> analyst, con el feedback como
      `current_instruction` (el campo que el analista ya lee).
    - Rechazado sin revisiones disponibles -> synthesizer, dejando el feedback
      en `pending_issues` para que la síntesis lo reporte como no resuelto.
    """
    step = state.get("step_count", 0)
    if decision.approved:
        note = "Aprobado por revisión humana" + (f": {decision.feedback}" if decision.feedback else "")
        return "synthesizer", _update(step, note, {})

    feedback = decision.feedback or DEFAULT_REJECT_FEEDBACK
    if can_revise(state):
        instruction = f"Revisión humana (rechazado): {feedback}. Corregí tu reporte según este pedido y mantené el resto igual."
        return "analyst", _update(step, f"Rechazado por revisión humana: {feedback}", {
            "next_agent": "analyst",
            "current_instruction": instruction,
            "supervisor_feedback": f"Revisión humana: {feedback}",
        })

    issue = f"Revisión humana rechazó el reporte pero no quedan revisiones del analista: {feedback}"
    pending = list(state.get("pending_issues", []) or []) + [issue]
    return "synthesizer", _update(step, issue, {"pending_issues": pending})


def _update(step: int, note: str, extra: dict) -> dict:
    return {
        **extra,
        "messages": [HumanMessage(content=note, name=REVIEWER)],
        "contributions": [make_contribution(REVIEWER, step, 1, "aprobar análisis", note)],
    }
