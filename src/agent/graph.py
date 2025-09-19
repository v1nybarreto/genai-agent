"""
Agente (LangGraph): orquestração dos nós de decisão, geração/validação/execução
de SQL e síntese de resposta.

Princípios de projeto
---------------------
- Nunca executa SQL sem *dry-run* válido.
- Logs informativos em cada etapa do fluxo.
- Tipagem e docstrings para facilitar manutenção.
- Funções de conveniência:
    - `build_graph()` → compila o grafo.
    - `run(question: str)` → executa e devolve a resposta final (str).
    - `run_debug(question: str)` → devolve o estado completo para depuração.
    - `reset_graph()` → limpa o cache do grafo compilado.

Dependências
------------
- Funções de negócio em `src.agent.nodes`.
- Logger opcional em `src.utils.logger.get_logger` (com fallback).
"""

from __future__ import annotations

from typing import Optional, TypedDict, Dict, Any
import os
from datetime import datetime, timezone
from threading import Lock

from langgraph.graph import StateGraph, END

# Logger (com fallback silencioso a logging básico)
try:
    from src.utils.logger import get_logger

    _log = get_logger(__name__)
except Exception:  # pragma: no cover - fallback simples
    import logging

    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
    _log = logging.getLogger(__name__)


# Nós de negócio
#  Cada função aqui importada deve ter interface estável.
#  Este módulo protege chamadas com try/except para manter robustez do fluxo.

from src.agent.nodes import (
    route_intent,
    generate_sql,
    validate_sql,
    execute_sql,
    synthesize,
    chitchat,
)

__all__ = [
    "AgentState",
    "GRAPH_VERSION",
    "build_graph",
    "run",
    "run_debug",
    "reset_graph",
]

GRAPH_VERSION = "1.3.1"


class AgentState(TypedDict, total=False):
    """
    Estado propagado entre os nós do grafo.

    Campos
    ------
    question : str
        Pergunta original do usuário.
    intent : str
        Intenção classificada pelo roteador: "data" | "chitchat".
    sql : str
        SQL gerado (quando intent = "data").
    validation_ok : bool
        Resultado do dry-run (True se válido).
    validation_error : Optional[str]
        Erro de validação/execução (quando houver).
    df : object
        Resultado tabular (por ex. pandas.DataFrame) da execução do SQL.
    answer : str
        Resposta final em linguagem natural.
    meta : dict
        Metadados auxiliares (p.ex. bytes do dry-run, versão do grafo, latência).
    """

    question: str
    intent: str
    sql: str
    validation_ok: bool
    validation_error: Optional[str]
    df: object
    answer: str
    meta: Dict[str, Any]


# Implementações embrulhadas dos nós do grafo
#   Nota: Cada nó captura exceções e escreve estado consistente para evitar
#   quebrar o grafo e facilitar a depuração em runtime.


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _node_router(state: AgentState) -> AgentState:
    """
    Roteia para análise de dados ou chitchat.

    Define:
        - state["intent"] em {"data","chitchat"} (default: "chitchat")
        - state["meta"]["graph_version"], ["started_at_utc"]
    """
    q = (state.get("question") or "").strip()
    state.setdefault("meta", {})
    state["meta"]["graph_version"] = GRAPH_VERSION
    state["meta"]["started_at_utc"] = (
        state["meta"].get("started_at_utc") or _utcnow_iso()
    )
    state["meta"]["latency_ms"] = state["meta"].get("latency_ms") or 0  # placeholder

    try:
        r = route_intent(q)
        intent = (r or {}).get("intent", "chitchat")
        if intent not in {"data", "chitchat"}:
            _log.warning("Router | intent inválida=%r, usando 'chitchat'", intent)
            intent = "chitchat"
    except Exception as e:
        _log.exception("Router | erro no route_intent: %r", e)
        intent = "chitchat"

    state["intent"] = intent
    _log.info("Router | intent=%s | q=%s", intent, q[:120])
    return state


def _node_sql_gen(state: AgentState) -> AgentState:
    """
    Gera SQL a partir da pergunta (usando gerador adaptado ao schema).

    Define:
        - state["sql"] (string possivelmente vazia em caso de falha)
        - state["meta"]["sql_preview"]
    """
    q = (state.get("question") or "").strip()
    sql = ""

    try:
        g = generate_sql(q)
        sql = (g or {}).get("sql", "") or ""
    except Exception as e:
        _log.exception("SQL Gen | erro no generate_sql: %r", e)

    # Proteção de log
    safe_sql_preview = sql.replace("\n", " ").replace("\r", " ")[:400]
    if not sql:
        _log.warning("SQL Gen | sql vazio")
    else:
        _log.info("SQL Gen | sql=%s", safe_sql_preview)

    state["sql"] = sql
    state.setdefault("meta", {})["sql_preview"] = safe_sql_preview
    return state


def _node_sql_validate(state: AgentState) -> AgentState:
    """
    Valida o SQL via dry-run. Se falhar, o fluxo seguirá para síntese
    com mensagem de erro (sem executar consulta custosa).

    Define:
        - state["validation_ok"] (bool)
        - state["validation_error"] (str | None)
        - state["meta"]["dry_run_bytes"] (int | None)
    """
    sql = state.get("sql") or ""
    ok = False
    err = None
    dry_run_bytes = None

    if not sql.strip():
        err = "SQL ausente após geração."
        _log.warning("Validate | %s", err)
    else:
        try:
            v = validate_sql(sql)
            ok = bool((v or {}).get("ok"))
            err = (v or {}).get("error")
            dry_run_bytes = (v or {}).get("dry_run_bytes")
        except Exception as e:
            ok = False
            err = f"Exceção no validate_sql: {e!r}"
            _log.exception("Validate | erro: %r", e)

    state["validation_ok"] = ok
    state["validation_error"] = err
    state.setdefault("meta", {})["dry_run_bytes"] = dry_run_bytes

    _log.info(
        "Validate | ok=%s | dry_run_bytes=%s | err=%s",
        ok,
        dry_run_bytes,
        (err or "")[:300],
    )
    return state


def _node_sql_exec(state: AgentState) -> AgentState:
    """
    Executa a consulta no BigQuery **somente** se a validação for ok.

    Define:
        - state["df"] (objeto tabular) quando sucesso.
        - state["meta"]["df_shape"] (tuple | None)
        - Em caso de falha, marca validation_ok=False e preenche validation_error.
    """
    if not state.get("validation_ok"):
        # Segurança adicional
        _log.warning("Execute | ignora execução pois validation_ok=False")
        return state

    sql = state.get("sql") or ""
    if not sql.strip():
        state["validation_ok"] = False
        state["validation_error"] = "SQL vazio no executor."
        _log.warning("Execute | SQL vazio; abortando.")
        return state

    try:
        out = execute_sql(sql) or {}
        ok = bool(out.get("ok"))
        state["df"] = out.get("df")

        if not ok:
            state["validation_ok"] = False
            state["validation_error"] = (
                out.get("error") or "Falha desconhecida na execução."
            )
            _log.warning(
                "Execute | failed | err=%s", (state["validation_error"] or "")[:300]
            )
        else:
            # Evita log gigante
            df = state.get("df")
            shape = getattr(df, "shape", None)
            state.setdefault("meta", {})["df_shape"] = shape
            _log.info("Execute | ok | df_shape=%s", shape)
    except Exception as e:
        state["validation_ok"] = False
        state["validation_error"] = f"Exceção no execute_sql: {e!r}"
        _log.exception("Execute | erro: %r", e)

    return state


def _node_synth(state: AgentState) -> AgentState:
    """
    Produz a resposta final.

    Regras:
        - Se houve falha na validação/execução, comunica o erro de forma clara.
        - Caso contrário, sintetiza com LLM (se habilitado) ou fallback determinístico.
        - Finaliza metadados de latência.
    """
    # Atalho: em caso de erro, retornar mensagem amigável
    if not state.get("validation_ok", True) and state.get("validation_error"):
        msg = (
            "Não consegui validar/executar a consulta. "
            f"Detalhes: {state['validation_error']}"
        )
        state["answer"] = msg
        _log.info("Synth | return_error")
    else:
        try:
            out = synthesize(state.get("df"), (state.get("question") or ""))
            state["answer"] = (out or {}).get("answer", "") or ""
            if not state["answer"]:
                # Fallback extra para nunca retornar string vazia
                state["answer"] = (
                    "Consegui processar sua solicitação, mas não gerei texto. Tente reformular a pergunta."
                )
            _log.info("Synth | ok")
        except Exception as e:
            _log.exception("Synth | erro em synthesize: %r", e)
            state["answer"] = (
                "Ocorreu um erro ao sintetizar a resposta. " f"Detalhes: {e!r}"
            )

    # Fechamento de latência
    meta = state.setdefault("meta", {})
    if "started_at_utc" not in meta:
        meta["started_at_utc"] = _utcnow_iso()
    meta["ended_at_utc"] = _utcnow_iso()
    try:
        # tolerante a valores faltantes
        start = datetime.fromisoformat(meta["started_at_utc"])
        end = datetime.fromisoformat(meta["ended_at_utc"])
        meta["latency_ms"] = int((end - start).total_seconds() * 1000)
    except Exception:
        pass

    return state


def _node_chitchat(state: AgentState) -> AgentState:
    """
    Atende mensagens de saudação/conversacionais usando LLM (ou fallback).
    Nunca lança exceção: sempre preenche `state['answer']`.
    """
    try:
        out = chitchat((state.get("question") or ""))
        state["answer"] = (out or {}).get("answer", "") or "Olá! Como posso ajudar?"
        _log.info("Chitchat | ok")
    except Exception as e:
        _log.exception("Chitchat | erro em chitchat: %r", e)
        state["answer"] = (
            "Olá! Não consegui responder agora, mas posso tentar novamente."
        )

    # Fechamento de latência também no ramo conversacional
    meta = state.setdefault("meta", {})
    if "started_at_utc" not in meta:
        meta["started_at_utc"] = _utcnow_iso()
    meta["ended_at_utc"] = _utcnow_iso()
    try:
        start = datetime.fromisoformat(meta["started_at_utc"])
        end = datetime.fromisoformat(meta["ended_at_utc"])
        meta["latency_ms"] = int((end - start).total_seconds() * 1000)
    except Exception:
        pass

    return state


# Construção e execução do grafo


def build_graph():
    """
    Constrói e *compila* o grafo LangGraph do agente.

    Returns
    -------
    langgraph.graph.CompiledGraph
        Grafo pronto para invocação com `.invoke(state_dict)`.
    """
    g = StateGraph(AgentState)

    # Registra nós
    g.add_node("router", _node_router)
    g.add_node("sql_gen", _node_sql_gen)
    g.add_node("sql_validate", _node_sql_validate)
    g.add_node("sql_exec", _node_sql_exec)
    g.add_node("synth", _node_synth)
    g.add_node("chitchat", _node_chitchat)

    # Entrada
    g.set_entry_point("router")

    # Roteamento por intenção
    g.add_conditional_edges(
        "router",
        lambda s: s.get("intent", "chitchat"),
        {"data": "sql_gen", "chitchat": "chitchat"},
    )

    # Branch de dados
    g.add_edge("sql_gen", "sql_validate")
    g.add_conditional_edges(
        "sql_validate",
        lambda s: "ok" if s.get("validation_ok") else "fail",
        {"ok": "sql_exec", "fail": "synth"},
    )
    g.add_edge("sql_exec", "synth")

    # Saídas
    g.add_edge("synth", END)
    g.add_edge("chitchat", END)

    app = g.compile()
    _log.info("Graph compiled | version=%s", GRAPH_VERSION)
    return app


# Cache do grafo compilado para evitar custo repetido
_APP = None
_APP_LOCK = Lock()


def _get_app():
    """
    Obtém (ou cria) o grafo compilado em cache.

    Returns
    -------
    langgraph.graph.CompiledGraph
    """
    global _APP
    if _APP is None:
        with _APP_LOCK:
            if _APP is None:
                _APP = build_graph()
    return _APP


def reset_graph() -> None:
    """
    Limpa o cache do grafo compilado. Útil em notebooks/hot-reload.
    """
    global _APP
    with _APP_LOCK:
        _APP = None
    _log.info("Graph cache resetado.")


def run(question: str) -> str:
    """
    Executa o fluxo completo e retorna a *resposta final*.

    Parameters
    ----------
    question : str
        Pergunta do(a) usuário(a) em linguagem natural.

    Returns
    -------
    str
        Resposta final sintetizada.
    """
    q = (question or "").strip()
    if not q:
        return "Por favor, forneça uma pergunta."

    try:
        app = _get_app()
        # inicializa o started_at para latência mesmo se o nó de router for pulado
        state_in: AgentState = {
            "question": q,
            "meta": {"started_at_utc": _utcnow_iso()},
        }
        final = app.invoke(state_in) or {}
        answer = final.get("answer", "") or ""
        return answer or "Processamento concluído, mas sem resposta textual."
    except Exception as e:  # pragma: no cover - proteção em runtime
        _log.exception("run | unhandled_error")
        return f"Ocorreu um erro inesperado ao processar sua solicitação: {e!r}"


def run_debug(question: str) -> AgentState:
    """
    Executa o fluxo e retorna **todo o estado** final para depuração.

    Útil para notebooks/logs quando você quiser inspecionar `sql`, `df`,
    `validation_ok`, etc.

    Parameters
    ----------
    question : str
        Pergunta em linguagem natural.

    Returns
    -------
    AgentState
        Dicionário com todos os campos do estado final.
    """
    q = (question or "").strip()
    app = _get_app()
    state_in: AgentState = {"question": q, "meta": {"started_at_utc": _utcnow_iso()}}
    return app.invoke(state_in)  # type: ignore
