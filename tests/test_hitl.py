"""Decisión humana pura (sin LLM ni Redis)."""

import pytest

from app.hitl import (ReviewDecision, build_interrupt_payload, can_revise, parse_decision,
                      resolve_review)
from state import MAX_RUNS_PER_AGENT, initial_state


def analyzed_state(analyst_runs=1, **extra):
    return initial_state("lodash 4.17.15 (npm)") | {
        "analysis_report": "## Ranking de riesgo\n| 1 | lodash |",
        "research_findings": "lodash: 3 vulns",
        "revisions": {"researcher": 1, "analyst": analyst_runs},
        "step_count": 3,
    } | extra


def test_payload_exposes_pending_report():
    payload = build_interrupt_payload(analyzed_state(pending_issues=["R2: x"]))
    assert payload["type"] == "approval_request"
    assert payload["analysis_report"].startswith("## Ranking")
    assert payload["pending_issues"] == ["R2: x"]
    assert payload["can_request_changes"] is True


@pytest.mark.parametrize("raw, expected", [
    ({"approved": True}, ReviewDecision(True, None)),
    ({"approved": False, "feedback": "  falta django  "}, ReviewDecision(False, "falta django")),
    ({"approved": False, "feedback": "   "}, ReviewDecision(False, None)),
    (True, ReviewDecision(True, None)),
])
def test_parse_decision(raw, expected):
    assert parse_decision(raw) == expected


@pytest.mark.parametrize("raw", [None, {}, {"approved": "yes"}, {"approved": True, "feedback": 3}, "ok"])
def test_parse_decision_rejects_invalid(raw):
    with pytest.raises(ValueError):
        parse_decision(raw)


def test_approved_goes_to_synthesizer():
    goto, update = resolve_review(analyzed_state(), ReviewDecision(True))
    assert goto == "synthesizer"
    assert "current_instruction" not in update
    assert update["contributions"][0]["agent"] == "human_review"


def test_rejected_sends_feedback_to_analyst():
    goto, update = resolve_review(analyzed_state(), ReviewDecision(False, "agregá el CVE de log4j"))
    assert goto == "analyst"
    assert "agregá el CVE de log4j" in update["current_instruction"]
    assert update["next_agent"] == "analyst"


def test_rejected_without_feedback_uses_default():
    goto, update = resolve_review(analyzed_state(), ReviewDecision(False))
    assert goto == "analyst" and "rechazó" in update["current_instruction"]


def test_rejected_without_revisions_left_goes_to_synthesizer_with_issue():
    st = analyzed_state(analyst_runs=MAX_RUNS_PER_AGENT, pending_issues=["previo"])
    assert not can_revise(st)
    goto, update = resolve_review(st, ReviewDecision(False, "más detalle"))
    assert goto == "synthesizer"
    assert update["pending_issues"][0] == "previo"
    assert "más detalle" in update["pending_issues"][-1]


def test_rejected_without_report_cannot_revise():
    st = initial_state("firmware de router") | {"research_findings": "FUERA DE ALCANCE"}
    goto, _ = resolve_review(st, ReviewDecision(False, "x"))
    assert goto == "synthesizer"
