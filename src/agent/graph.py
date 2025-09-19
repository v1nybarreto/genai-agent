"""
Grafo do agente (LangGraph): roteador -> gerador SQL -> validador -> executor -> sintetizador.

Contrato público:
- build_graph() -> compilado do LangGraph pronto para .invoke(state)
- run(question: str) -> str  (atalho: executa o fluxo completo e retorna 'answer')

Dependências:
- src/agent/nodes.py  (funções de negócio)
"""

from __future__ import annotations

from typing import TypedDict, Optional
from langgraph.graph import StateGraph, END

from src.agent.nodes import (
    route_intent,
    generate_sql,
    validate_sql,
    execute_sql,
    synthesize,
)


class AgentState(TypedDict, total=False):
    """
    Estado compartilhado entre nós do grafo.
    Campos opcionais são preenchidos progressivamente pelos nós.
    """
    question: str
    intent: str
    sql: str
    validation_ok: bool
    validation_error: Optional[str]
    df: object
    answer: str


# ------------------------- NÓS -------------------------

def _node_router(state: AgentState) -> AgentState:
    """
    Classifica a intenção da pergunta:
      - 'data'      -> segue para geração de SQL
      - 'chitchat'  -> segue para síntese direta (sem acessar dados)
    """
    q = state.get("question", "")
    r = route_intent(q)
    state["intent"] = r["intent"]
    return state


def _node_sql_gen(state: AgentState) -> AgentState:
    """Gera SQL a partir da pergunta (regra atual: gerador adaptativo ao schema)."""
    q = state.get("question", "")
    g = generate_sql(q)
    state["sql"] = g["sql"]
    return state


def _node_sql_validate(state: AgentState) -> AgentState:
    """Valida o SQL via DRY-RUN (sem custo)."""
    v = validate_sql(state["sql"])
    state["validation_ok"] = bool(v["ok"])
    state["validation_error"] = v.get("error")
    return state


def _node_sql_exec(state: AgentState) -> AgentState:
    """
    Executa a consulta no BigQuery se a validação passou.
    Em caso de falha de execução, propaga erro para o sintetizador.
    """
    out = execute_sql(state["sql"])
    state["df"] = out.get("df")
    if not out.get("ok"):
        state["validation_ok"] = False
        state["validation_error"] = out.get("error")
    return state


def _node_synth(state: AgentState) -> AgentState:
    """
    Síntese da resposta:
    - Se a intenção for 'chitchat', responde diretamente sem acessar dados.
    - Se houve falha de validação/execução, retorna mensagem clara.
    - Caso contrário, delega ao sintetizador baseado em DataFrame.
    """
    # Tratamento direto para conversas breves (saudação etc.)
    if state.get("intent") == "chitchat":
        q = (state.get("question") or "").strip().lower()
        greetings = ("olá", "ola", "oi", "bom dia", "boa tarde", "boa noite")
        if any(g in q for g in greetings):
            state["answer"] = (
                "Olá! Posso ajudar com análises sobre os chamados do 1746. "
                "Exemplo: 'Quantos chamados houve em 28/11/2024?'"
            )
        else:
            state["answer"] = (
                "Posso ajudar com análises sobre os chamados do 1746. "
                "Qual informação você precisa?"
            )
        return state

    # Mensagem clara em caso de falha de validação/execução SQL
    if not state.get("validation_ok", True) and state.get("validation_error"):
        state["answer"] = (
            "Não consegui validar/executar a consulta. "
            f"Detalhes: {state['validation_error']}"
        )
        return state

    # Síntese baseada em DataFrame (camada de negócio em nodes.synthesize)
    q = state.get("question", "")
    out = synthesize(state.get("df"), q)
    state["answer"] = out["answer"]
    return state


# ------------------------- CONSTRUÇÃO DO GRAFO -------------------------

def build_graph():
    """Constroi e compila o grafo LangGraph do agente."""
    g = StateGraph(AgentState)

    # Nós
    g.add_node("router", _node_router)
    g.add_node("sql_gen", _node_sql_gen)
    g.add_node("sql_validate", _node_sql_validate)
    g.add_node("sql_exec", _node_sql_exec)
    g.add_node("synth", _node_synth)

    # Entrada
    g.set_entry_point("router")

    # router -> (data|chitchat)
    g.add_conditional_edges(
        "router",
        lambda s: s["intent"],
        {"data": "sql_gen", "chitchat": "synth"},
    )

    # Ramo 'data'
    g.add_edge("sql_gen", "sql_validate")
    g.add_conditional_edges(
        "sql_validate",
        lambda s: "ok" if s.get("validation_ok") else "fail",
        {"ok": "sql_exec", "fail": "synth"},
    )
    g.add_edge("sql_exec", "synth")

    # Saída
    g.add_edge("synth", END)

    return g.compile()


# ------------------------- ATALHO DE EXECUÇÃO -------------------------

def run(question: str) -> str:
    """
    Executa o fluxo completo no LangGraph e retorna a 'answer' (string).

    Exemplo:
        print(run("Quantos chamados foram abertos no dia 28/11/2024?"))
    """
    app = build_graph()
    final = app.invoke({"question": question})
    return final.get("answer", "")