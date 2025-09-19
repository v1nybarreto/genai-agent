"""
Testes de fluxo de chit-chat (conversa genérica) do agente.

Objetivo
--------
Validar que o agente responde adequadamente a mensagens não-analíticas,
usando LLM quando habilitado e configurado, ou o fallback estático
quando o LLM não está disponível.

Critérios validados
-------------------
- Se OPENAI_API_KEY estiver definido e LLM_USE_FOR_SYNTH=1:
    → A resposta deve vir do LLM (mensagens naturais de saudação).
- Caso contrário:
    → A resposta deve vir do fallback estático definido em nodes.chitchat.
"""

import os
import pytest
from src.agent.graph import run


@pytest.mark.parametrize("msg", ["Olá, tudo bem?", "Oi, agente!"])
def test_chitchat_flow(msg):
    """
    Valida que o agente responde a mensagens de chit-chat.

    Parâmetros
    ----------
    msg : str
        Mensagem de teste simulando uma saudação/conversa genérica.
    """
    ans = run(msg)

    # Sempre deve retornar string não vazia
    assert isinstance(ans, str)
    assert len(ans.strip()) > 0

    # Cenário com LLM habilitado (OPENAI_API_KEY presente + LLM_USE_FOR_SYNTH=1)
    if os.getenv("OPENAI_API_KEY") and os.getenv("LLM_USE_FOR_SYNTH") == "1":
        # Esperamos resposta natural do LLM (provavelmente inclui saudação simples)
        assert "olá" in ans.lower() or "oi" in ans.lower()
    else:
        # Cenário de fallback estático → deve citar contexto do agente (1746)
        assert "1746" in ans or "chamados" in ans.lower()
