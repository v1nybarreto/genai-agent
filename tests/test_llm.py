"""
Teste mínimo para utils/llm.py
Valida que conseguimos obter resposta de um LLM via OpenAI.
"""

import os
import pytest
from src.utils.llm import get_llm_response

@pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY não definido")
def test_llm_basic():
    resp = get_llm_response("Olá, tudo bem?")
    assert resp["ok"] is True
    assert isinstance(resp["text"], str)
    assert len(resp["text"]) > 0
