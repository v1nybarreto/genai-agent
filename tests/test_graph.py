"""
Testes mínimos de construção e execução do grafo do agente.

Objetivos
---------
- Garantir que o grafo compila sem erros.
- Validar que a função `run()` retorna sempre uma string não-vazia,
  mesmo em casos simples de chit-chat (com ou sem LLM habilitado).

Esses testes funcionam como um "smoke test" rápido,
sem depender de execução de SQL complexa.
"""

from src.agent.graph import build_graph, run


def _nonempty_str(x) -> bool:
    """Helper: retorna True se x for string não-vazia (ignorando espaços)."""
    return isinstance(x, str) and len((x or "").strip()) > 0


def test_build_graph_compiles():
    """O grafo deve ser construído corretamente via build_graph()."""
    app = build_graph()
    # Deve retornar um objeto compilado (não None)
    assert app is not None


def test_run_answer_str():
    """
    `run()` deve retornar uma resposta textual não-vazia
    para uma pergunta genérica de chit-chat.
    """
    ans = run("Olá, tudo bem?")
    assert _nonempty_str(ans)
