"""
src/utils/llm.py
----------------

Camada fina para chamadas ao LLM (OpenAI) com:
- Leitura de variáveis de ambiente
- Logger padronizado
- Timeout e tentativas (retries) com backoff simples
- Interface estável: get_llm_response(prompt) -> {ok, text, error}
- Fail-safe (nunca levanta exceção; sempre retorna dicionário padronizado)

Dependências de ambiente
-----------------------
- LLM_PROVIDER=OPENAI
- OPENAI_MODEL=gpt-4o-mini                 # modelo default (sobreponível)
- OPENAI_API_KEY=<sua chave>               # obrigatório para habilitar
- OPENAI_BASE_URL=<url opcional>           # p/ proxies/self-hosted compatíveis
- OPENAI_ORG=<org opcional>                # se aplicável

Tuning opcional por ENV
-----------------------
- LLM_ENABLED=1                            # "0" desativa forçadamente (default: 1)
- LLM_MAX_TOKENS=220
- LLM_TEMPERATURE=0.2
- LLM_TIMEOUT=30                           # segundos
- LLM_MAX_RETRIES=2
- LLM_PROMPT_MAX_CHARS=8000                # truncamento defensivo do prompt

Uso
---
    from src.utils.llm import get_llm_response
    out = get_llm_response("Olá, tudo bem?")
    if out["ok"]:
        print(out["text"])
"""

from __future__ import annotations

from typing import Dict, Any, Optional
import os
import time

from src.utils.logger import get_logger

_LOG = get_logger(__name__)


# Configuração por ambiente (carregada em import; pode ser recarregada via reset)


def _load_env() -> Dict[str, Any]:
    return {
        "PROVIDER": (os.getenv("LLM_PROVIDER") or "OPENAI").strip().upper(),
        "ENABLED": (os.getenv("LLM_ENABLED") or "1").strip() != "0",
        "MODEL": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        "API_KEY": os.getenv("OPENAI_API_KEY"),
        "BASE_URL": os.getenv("OPENAI_BASE_URL") or None,
        "ORG": os.getenv("OPENAI_ORG") or None,
        "MAX_TOKENS": int(os.getenv("LLM_MAX_TOKENS", "220")),
        "TEMPERATURE": float(os.getenv("LLM_TEMPERATURE", "0.2")),
        "TIMEOUT_S": int(os.getenv("LLM_TIMEOUT", "30")),
        "MAX_RETRIES": int(os.getenv("LLM_MAX_RETRIES", "2")),
        "PROMPT_MAX_CHARS": int(os.getenv("LLM_PROMPT_MAX_CHARS", "8000")),
    }


_CFG = _load_env()

# Cliente é inicializado sob demanda
_client = None
_client_ready_err: Optional[str] = None


def reset_llm_client() -> None:
    """
    Recarrega variáveis de ambiente e reinicializa o cliente LLM.
    Útil ao mudar API key/flags durante testes.
    """
    global _client, _client_ready_err, _CFG
    _CFG = _load_env()
    _client = None
    _client_ready_err = None
    _LOG.info("LLM client resetado (recarregadas variáveis de ambiente).")


def _init_client_if_needed() -> None:
    """
    Inicializa o cliente do provedor escolhido sob demanda (lazy).
    Preenche _client ou _client_ready_err. Não lança exceções.
    """
    global _client, _client_ready_err

    if _client is not None or _client_ready_err is not None:
        return

    if not _CFG["ENABLED"]:
        _client_ready_err = "LLM disabled by env (LLM_ENABLED=0)"
        _LOG.info("LLM desativado por configuração de ambiente.")
        return

    if _CFG["PROVIDER"] != "OPENAI":
        _client_ready_err = f"Unsupported LLM provider: {_CFG['PROVIDER']}"
        _LOG.warning(_client_ready_err)
        return

    if not _CFG["API_KEY"]:
        _client_ready_err = "OPENAI_API_KEY not set"
        _LOG.warning("LLM não configurado: %s", _client_ready_err)
        return

    try:
        # Import local para evitar dependência quando não habilitado
        from openai import OpenAI

        # Permite uso de proxies/self-hosted compatíveis via base_url
        _client = OpenAI(
            api_key=_CFG["API_KEY"], base_url=_CFG["BASE_URL"], organization=_CFG["ORG"]
        )
        _LOG.info(
            "LLM habilitado | provider=%s | model=%s | base_url=%s | org=%s",
            _CFG["PROVIDER"],
            _CFG["MODEL"],
            bool(_CFG["BASE_URL"]),
            bool(_CFG["ORG"]),
        )
    except Exception as e:
        _client_ready_err = f"Falha ao inicializar cliente OpenAI: {repr(e)}"
        _client = None
        _LOG.error(_client_ready_err)


def _truncate_prompt(prompt: str) -> str:
    """Trunca o prompt para um limite defensivo de caracteres."""
    if not prompt:
        return ""
    p = prompt.strip()
    if len(p) <= _CFG["PROMPT_MAX_CHARS"]:
        return p
    _LOG.debug("Truncando prompt de %d para %d chars", len(p), _CFG["PROMPT_MAX_CHARS"])
    return p[: _CFG["PROMPT_MAX_CHARS"]]


def _try_openai_responses_call(safe_prompt: str) -> Optional[str]:
    """
    Tenta usar a API 'responses' (SDKs mais novos). Se não existir, retorna None.
    """
    try:
        # Alguns SDKs expõem client.responses.create()
        create = getattr(_client, "responses").create
        resp = create(
            model=_CFG["MODEL"],
            input=[
                {
                    "role": "system",
                    "content": "Responda em PT-BR, com clareza e objetividade.",
                },
                {"role": "user", "content": safe_prompt},
            ],
            temperature=_CFG["TEMPERATURE"],
            max_output_tokens=_CFG["MAX_TOKENS"],
        )
        # Normalização leve do retorno
        text = None
        try:
            if hasattr(resp, "output_text"):
                text = (resp.output_text or "").strip()
        except Exception:
            pass
        if not text:
            # fallback de extração
            text = (getattr(resp, "content", None) or "").strip()
        return text or None
    except AttributeError:
        return None  # API responses não disponível
    except Exception as e:
        _LOG.warning("OpenAI responses API falhou: %r", e)
        return None


def _openai_chat_call(safe_prompt: str, timeout_s: int) -> Optional[str]:
    """
    Usa a API chat.completions (compatível com SDKs mais antigos e atuais).
    Tenta usar request-timeout context manager quando disponível.
    """
    try:
        # SDK
        request_timeout_ctx = getattr(
            getattr(_client, "_client", None), "request_timeout", None
        )
    except Exception:
        request_timeout_ctx = None

    try:
        if callable(request_timeout_ctx):
            # usa context manager para garantir timeout de rede
            with request_timeout_ctx(timeout_s):
                resp = _client.chat.completions.create(
                    model=_CFG["MODEL"],
                    messages=[
                        {
                            "role": "system",
                            "content": "Responda em PT-BR, com clareza e objetividade.",
                        },
                        {"role": "user", "content": safe_prompt},
                    ],
                    temperature=_CFG["TEMPERATURE"],
                    max_tokens=_CFG["MAX_TOKENS"],
                )
        else:
            # fallback
            resp = _client.chat.completions.create(
                model=_CFG["MODEL"],
                messages=[
                    {
                        "role": "system",
                        "content": "Responda em PT-BR, com clareza e objetividade.",
                    },
                    {"role": "user", "content": safe_prompt},
                ],
                temperature=_CFG["TEMPERATURE"],
                max_tokens=_CFG["MAX_TOKENS"],
                timeout=timeout_s,
            )

        choice = (getattr(resp, "choices", None) or [None])[0]
        if choice and getattr(choice, "message", None):
            return (choice.message.content or "").strip()
        return None
    except Exception as e:
        _LOG.warning("OpenAI chat.completions falhou: %r", e)
        return None


def get_llm_response(
    prompt: str, *, max_retries: int = None, timeout_s: int = None
) -> Dict[str, Any]:
    """
    Envia um prompt ao LLM e retorna dicionário padronizado. Nunca lança exceção.

    Parameters
    ----------
    prompt : str
        Texto da solicitação ao LLM.
    max_retries : int | None
        Número de tentativas em caso de falha transitória. Default: LLM_MAX_RETRIES.
    timeout_s : int | None
        Tempo máximo por chamada (melhor-esforço). Default: LLM_TIMEOUT.

    Returns
    -------
    Dict[str, Any]
        ok   : bool        -> sucesso da chamada
        text : str | None  -> resposta do LLM (se ok=True)
        error: str | None  -> descrição do erro (se ok=False)
    """
    _init_client_if_needed()
    if _client is None:
        # Retorna motivo de indisponibilidade
        return {
            "ok": False,
            "text": None,
            "error": _client_ready_err or "LLM client not configured",
        }

    safe_prompt = _truncate_prompt(prompt or "")
    if not safe_prompt:
        return {"ok": False, "text": None, "error": "Prompt vazio."}

    retries = _CFG["MAX_RETRIES"] if max_retries is None else max_retries
    timeout = _CFG["TIMEOUT_S"] if timeout_s is None else timeout_s

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            start = time.time()

            # 1) Tenta 'responses'
            text = _try_openai_responses_call(safe_prompt)
            if not text:
                # 2) Fallback p/ chat.completions
                text = _openai_chat_call(safe_prompt, timeout)

            latency_ms = (time.time() - start) * 1000
            _LOG.debug(
                "LLM tentativa %d/%d | model=%s | latency_ms=%.0f",
                attempt,
                retries,
                _CFG["MODEL"],
                latency_ms,
            )

            if text:
                return {"ok": True, "text": text, "error": None}

            last_err = "Resposta vazia do LLM"
            _LOG.warning("LLM tentativa %d/%d: %s", attempt, retries, last_err)

        except Exception as e:
            last_err = repr(e)
            _LOG.warning("LLM tentativa %d/%d falhou: %s", attempt, retries, last_err)

        # Backoff simples e curto; ajustável conforme necessidade
        time.sleep(0.6 * attempt)

    return {"ok": False, "text": None, "error": last_err or "unknown error"}
