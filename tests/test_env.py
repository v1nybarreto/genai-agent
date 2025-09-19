"""
Testes de variáveis de ambiente críticas para o agente GenAI.

Objetivo
--------
Garantir que o ambiente está configurado de forma consistente
e compatível com os requisitos do desafio técnico.

Critérios validados
-------------------
- LLM_PROVIDER deve ser "OPENAI".
- OPENAI_MODEL deve ser "gpt-4o-mini".
- PROJECT_ID deve estar definido (ex.: "genai-rio").
- OPENAI_API_KEY deve estar definido:
  * Se LLM_USE_FOR_SYNTH=1 → não pode estar vazio.
  * Caso contrário → pode estar vazio, mas deve existir.
"""

import os


def test_env_vars():
    """Valida variáveis críticas para operação do agente."""
    # Provider e modelo padrão
    assert os.getenv("LLM_PROVIDER") == "OPENAI"
    assert os.getenv("OPENAI_MODEL") == "gpt-4o-mini"

    # Projeto sempre deve estar definido
    assert os.getenv("PROJECT_ID") is not None

    # Validação condicional da chave de API
    key = os.getenv("OPENAI_API_KEY")
    synth = os.getenv("LLM_USE_FOR_SYNTH")

    if synth == "1":
        # Se a síntese via LLM está ativa, a chave precisa estar preenchida
        assert key is not None and key.strip() != ""
    else:
        # Caso contrário, basta a variável existir (mesmo que vazia)
        assert key is not None


def test_defaults_safety():
    """
    Garante que valores default (quando definidos em pytest.ini/.env.example)
    não levam a comportamento inseguro.
    """
    # O provider deve ser restrito a OPENAI (sem fallback silencioso)
    assert os.getenv("LLM_PROVIDER") == "OPENAI"

    # Modelo default deve ser gpt-4o-mini
    assert os.getenv("OPENAI_MODEL") == "gpt-4o-mini"

    # PROJECT_ID não deve ser string vazia
    assert os.getenv("PROJECT_ID") and os.getenv("PROJECT_ID").strip() != ""
