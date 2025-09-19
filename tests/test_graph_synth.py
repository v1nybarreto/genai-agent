"""
Teste de integração para a síntese de respostas via LLM.

Objetivo
--------
Validar que, quando o LLM está habilitado, o agente consegue
transformar resultados do BigQuery em texto coerente e útil
ao domínio do 1746.

Regras de execução
------------------
- O teste só roda quando:
  * OPENAI_API_KEY está definida
  * LLM_USE_FOR_SYNTH == "1"
- Caso contrário, o teste é automaticamente pulado.

Critérios validados
-------------------
- `run()` deve retornar string não vazia.
- O texto deve ter vocabulário coerente ao tema da pergunta
  (sem exigir frase exata).
"""

import os
import pytest
from src.agent.graph import run

# Marca condicional: só executa se LLM estiver configurado
pytestmark = pytest.mark.skipif(
    not (os.getenv("OPENAI_API_KEY") and os.getenv("LLM_USE_FOR_SYNTH") == "1"),
    reason="LLM_USE_FOR_SYNTH não habilitado ou API key não configurada",
)


@pytest.mark.timeout(60)
def test_graph_synthesis_llm():
    """
    Executa pergunta real de dados e valida a síntese via LLM.
    """
    pergunta = (
        "Quais os 3 bairros que mais tiveram chamados abertos "
        "sobre reparo de buraco em 2023?"
    )

    resp = run(pergunta)

    # Deve retornar texto não vazio
    assert isinstance(resp, str)
    assert len(resp.strip()) > 0

    # Checagem leve de coerência temática (tokens esperados no domínio)
    low = resp.lower()
    assert any(
        tok in low for tok in ["buraco", "bairro", "bairros", "reparo"]
    ), f"Resposta incoerente com domínio esperado: {resp}"

    # Resposta não deve ser puramente numérica/trivial
    assert not resp.strip().isdigit()
