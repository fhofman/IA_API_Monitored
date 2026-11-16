"""Grafo de la API: el mismo orquestador del CLI + un nodo de aprobación humana.

    START -> supervisor --> researcher / analyst --> supervisor
                       \\--(FINISH)--> human_review --(aprobado)--> synthesizer --> END
                                                   \\--(rechazado)--> analyst

`human_review` pausa con `interrupt()` y se reanuda con `Command(resume=...)`.
Necesita checkpointer (Redis en el worker), por eso se inyecta en
`build_api_graph`. Importar este módulo construye los agentes (requiere
OPENAI_API_KEY): el worker lo importa recién después de `setup_tracing()`.
"""

from typing import Literal

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from agents.analyst_agent import analyst_node
from agents.research_agent import research_node
from agents.supervisor import supervisor_node
from agents.synthesizer import synthesizer_node
from app.hitl import build_interrupt_payload, parse_decision, resolve_review
from graph import route_from_supervisor
from state import MAX_STEPS, AgentState

# Margen extra sobre el CLI (MAX_STEPS * 3) por los pasos de human_review.
RECURSION_LIMIT = MAX_STEPS * 3 + 4


def route_api(state: AgentState) -> Literal["researcher", "analyst", "human_review"]:
    dest = route_from_supervisor(state)
    return "human_review" if dest == "synthesizer" else dest


def human_review_node(state: AgentState) -> Command[Literal["synthesizer", "analyst"]]:
    # Al reanudar, el nodo se reejecuta desde el principio: no hay LLM acá.
    decision = parse_decision(interrupt(build_interrupt_payload(state)))
    goto, update = resolve_review(state, decision)
    return Command(goto=goto, update=update)


def build_api_graph(checkpointer):
    builder = StateGraph(AgentState)
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("researcher", research_node)
    builder.add_node("analyst", analyst_node)
    builder.add_node("human_review", human_review_node)
    builder.add_node("synthesizer", synthesizer_node)

    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges("supervisor", route_api)
    builder.add_edge("researcher", "supervisor")
    builder.add_edge("analyst", "supervisor")
    builder.add_edge("synthesizer", END)

    return builder.compile(checkpointer=checkpointer)


def run_config(job_id: str) -> dict:
    return {
        "configurable": {"thread_id": job_id},
        "recursion_limit": RECURSION_LIMIT,
        "run_name": "vuln-orchestrator-api",
    }
