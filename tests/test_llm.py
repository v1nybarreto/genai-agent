"""
Teste mínimo de integração para utils/llm.py.

Objetivo
--------
Validar que conseguimos obter resposta de um LLM via OpenAI,
quando a variável de ambiente OPENAI_API_KEY está configurada.

Regras
------
- Só roda se OPENAI_API_KEY estiver definida.
- Espera-se retorno de dicionário padronizado: {ok, text, error}.
- Texto deve ser não-vazio e coerente (sem validar conteúdo exato).
"""

import os
import pytest
from src.utils.llm import get_llm_response

# Pula automaticamente se não houver chave de API
pytestmark = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY não definido"
)


@pytest.mark.timeout(45)
def test_llm_basic():
    """
    O LLM deve responder com texto não-vazio
    a uma saudação simples.
    """
    resp = get_llm_response("Olá, tudo bem?")

    # Estrutura esperada
    assert isinstance(resp, dict)
    assert "ok" in resp and "text" in resp and "error" in resp

    # Deve ter sucesso
    assert resp["ok"] is True

    # Texto não-vazio
    text = resp["text"]
    assert isinstance(text, str)
    assert len(text.strip()) > 0

    # Quando sucesso, error deve ser None
    assert resp["error"] is None
