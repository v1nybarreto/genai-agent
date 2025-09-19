"""
src/utils/logger.py
-------------------

Logger utilitário com formatação consistente e nível configurável por variáveis
de ambiente.

Configuração por ENV
--------------------
- LOG_LEVEL   : nível mínimo de log (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                Default = INFO.
- LOG_FMT     : formato customizado (opcional).
- LOG_DATEFMT : formato da data (opcional).

Exemplo de uso
--------------
    from src.utils.logger import get_logger

    log = get_logger(__name__)
    log.info("Iniciado")
    log.warning("Atenção")
    log.error("Falhou", exc_info=True)
"""

from __future__ import annotations

import logging
import os
from typing import Optional


# Função principal


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Retorna um logger configurado com handler único e formato consistente.

    Parameters
    ----------
    name : Optional[str]
        Nome do logger (geralmente __name__). Se None, usa "app".

    Returns
    -------
    logging.Logger
        Instância pronta para uso, com nível e formato definidos por env.
    """
    # 1) Define nível a partir de LOG_LEVEL
    level_str = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, level_str, logging.INFO)

    # 2) Define formato de log
    log_fmt = (
        os.getenv("LOG_FMT") or "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    date_fmt = os.getenv("LOG_DATEFMT") or "%Y-%m-%d %H:%M:%S"

    # 3) Cria logger nomeado
    logger = logging.getLogger(name or "app")

    # 4) Evita múltiplos handlers em reimportações
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt=log_fmt, datefmt=date_fmt))
        logger.addHandler(handler)
        logger.propagate = False

    # 5) Aplica nível
    logger.setLevel(level)
    return logger
