"""Grafo e nós do agente (LangGraph).

Exporta utilitários de alto nível:
- build_graph()  → compila o grafo
- run()          → executa e retorna a resposta final (str)
- run_debug()    → executa e retorna o estado completo (dict)
- GRAPH_VERSION  → versão do grafo para rastreio
"""

from .graph import build_graph, run, run_debug, GRAPH_VERSION

__all__ = ["build_graph", "run", "run_debug", "GRAPH_VERSION"]
