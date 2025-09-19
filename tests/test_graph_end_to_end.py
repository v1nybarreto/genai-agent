"""
Testes end-to-end do grafo do agente.

Cobertura
---------
- Roteamento para DADOS e CHITCHAT.
- Geração/validação de SQL (sem depender do texto exato).
- Resposta final não vazia em ambos os ramos (LLM on/off).
- Telemetria mínima em state.meta (latência, versão do grafo, bytes do dry-run).
- Higiene de SQL: sem 'SELECT *'.
"""

from __future__ import annotations

import re
import pytest

from src.agent.graph import run, run_debug, GRAPH_VERSION


_SELECT_STAR = re.compile(r"select\s+\*\s", re.IGNORECASE)


def _nonempty_str(x) -> bool:
    return isinstance(x, str) and len((x or "").strip()) > 0


def test_graph_version_exposed():
    """A versão do grafo deve estar exposta (ajuda no rastreio de breaking changes)."""
    assert _nonempty_str(GRAPH_VERSION)


@pytest.mark.timeout(45)
def test_data_route_end_to_end():
    """
    Pergunta de dados → ramo 'data':
    - intent = 'data'
    - SQL não-vazio e com SELECT/FROM (sem SELECT *)
    - dry-run sinalizado (ok ou erro, mas campo presente)
    - answer não-vazia
    - meta traz telemetria básica
    """
    q = "Quantos chamados foram abertos no dia 28/11/2024?"
    state = run_debug(q)

    # Roteamento
    assert state.get("intent") == "data"

    # SQL gerado
    sql = state.get("sql", "")
    assert _nonempty_str(sql)
    sl = sql.lower()
    assert "select" in sl and "from" in sl
    assert not _SELECT_STAR.search(sql), "SQL não deve usar SELECT *"

    # Validação (sempre deve preencher os campos)
    assert "validation_ok" in state
    assert "validation_error" in state

    # Resposta final
    answer = state.get("answer", "")
    assert _nonempty_str(answer)

    # Telemetria (opcional porém útil)
    meta = state.get("meta", {}) or {}
    assert meta.get("graph_version") == GRAPH_VERSION
    # dry_run_bytes pode ser None em erro de validação, mas a chave deve existir:
    assert "dry_run_bytes" in meta
    # latência/meta podem não existir dependendo do run, então validamos presença opcional:
    # if present, deve ser coerente
    if "latency_ms" in meta:
        assert isinstance(meta["latency_ms"], (int, float))


@pytest.mark.timeout(45)
def test_chitchat_route_end_to_end():
    """
    Pergunta de chitchat → ramo 'chitchat':
    - intent = 'chitchat'
    - answer não-vazia (usa LLM se houver chave; caso contrário, fallback)
    """
    q = "Olá, tudo bem?"
    state = run_debug(q)

    assert state.get("intent") == "chitchat"
    assert _nonempty_str(state.get("answer", ""))


@pytest.mark.timeout(60)
def test_run_convenience_returns_string():
    """
    `run()` deve sempre retornar string amigável ao usuário,
    independentemente do caminho seguido.
    """
    a1 = run("Quais os 3 bairros com mais chamados de reparo de buraco em 2023?")
    a2 = run("Oi!")

    assert _nonempty_str(a1)
    assert _nonempty_str(a2)
