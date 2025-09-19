"""
Testes mínimos do grafo do agente.
"""

from src.agent.graph import build_graph, run

def test_build_graph_compiles():
    app = build_graph()
    assert app is not None

def test_run_answer_str():
    ans = run("Olá, tudo bem?")
    assert isinstance(ans, str)
    assert len(ans) > 0  # sintetizador