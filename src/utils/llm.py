"""
utils/llm.py — Cliente OpenAI e função utilitária.
Modelo padrão: gpt-4o-mini.
"""

from __future__ import annotations
import os
from typing import Optional, Dict, Any


class LLMDisabled(RuntimeError):
    """Indica que o LLM está desativado (sem OPENAI_API_KEY)."""


def get_llm_response(prompt: str,
                     model: Optional[str] = None,
                     temperature: float = 0.2,
                     max_tokens: int = 220) -> Dict[str, Any]:
    """
    Retorna {'ok': bool, 'text': str|None, 'error': str|None}.
    Cria o cliente OpenAI apenas na chamada (lazy), usando OPENAI_API_KEY do ambiente.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise LLMDisabled("OPENAI_API_KEY não definido; LLM desativado.")

    try:
        from openai import OpenAI  # import aqui para falhar cedo se o pacote não existir
        client = OpenAI(api_key=api_key)
        model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        text = (resp.choices[0].message.content or "").strip()
        return {"ok": True, "text": text, "error": None}
    except Exception as e:
        return {"ok": False, "text": None, "error": str(e)}
