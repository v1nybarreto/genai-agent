"""
src/utils/llm.py
----------------

Camada fina para chamadas ao LLM (OpenAI) com:
- Leitura de variáveis de ambiente
- Logger padronizado
- Timeout e tentativas (retries)
- Interface simples/estável: get_llm_response(prompt) -> {ok, text, error}

Dependências de ambiente:
- LLM_PROVIDER=OPENAI
- OPENAI_MODEL=gpt-4o-mini
- OPENAI_API_KEY=<sua chave>

Uso:
    from src.utils.llm import get_llm_response
    out = get_llm_response("Olá, tudo bem?")
    if out["ok"]:
        print(out["text"])
"""

from __future__ import annotations

from typing import Dict, Any
import os
import time

from openai import OpenAI

from src.utils.logger import get_logger

_LOG = get_logger(__name__)

_PROVIDER = os.getenv("LLM_PROVIDER", "OPENAI").upper()
_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_API_KEY = os.getenv("OPENAI_API_KEY")

# Cliente é inicializado apenas se houver provider + api key válidos.
_client = None
if _PROVIDER == "OPENAI" and _API_KEY:
    try:
        _client = OpenAI(api_key=_API_KEY)
        _LOG.info("LLM habilitado | provider=%s | model=%s", _PROVIDER, _MODEL)
    except Exception as e:
        _LOG.error("Falha ao inicializar cliente OpenAI: %r", e)
        _client = None
else:
    _LOG.warning(
        "LLM desativado: provider=%s, api_key_definida=%s",
        _PROVIDER,
        bool(_API_KEY),
    )


def get_llm_response(prompt: str, *, max_retries: int = 2, timeout_s: int = 30) -> Dict[str, Any]:
    """
    Envia um prompt simples ao modelo de chat e retorna dicionário padronizado.
    Nunca levanta exceção para o chamador.

    Parâmetros
    ----------
    prompt : str
        Texto da solicitação ao LLM.
    max_retries : int
        Número de tentativas em caso de falha transitória.
    timeout_s : int
        Tempo máximo por chamada (quando suportado pelo client).

    Retorno
    -------
    Dict[str, Any] com chaves:
        ok: bool        -> sucesso da chamada
        text: str|None  -> resposta do LLM (se ok=True)
        error: str|None -> descrição do erro (se ok=False)
    """
    if not _client:
        return {"ok": False, "text": None, "error": "LLM client not configured"}

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            start = time.time()
            resp = _client.chat.completions.create(
                model=_MODEL,
                messages=[
                    {"role": "system", "content": "Responda em PT-BR, com clareza e objetividade."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,   # respostas estáveis/baratas
                max_tokens=220,    # controle de custo
                timeout=timeout_s, # evita travar
            )
            text = (resp.choices[0].message.content or "").strip()
            _LOG.debug("LLM ok | model=%s | latency_ms=%.0f", _MODEL, (time.time() - start) * 1000)
            return {"ok": True, "text": text, "error": None}
        except Exception as e:
            last_err = repr(e)
            _LOG.warning("LLM tentativa %d/%d falhou: %s", attempt, max_retries, last_err)
            time.sleep(0.6 * attempt)  # backoff simples

    return {"ok": False, "text": None, "error": last_err or "unknown error"}