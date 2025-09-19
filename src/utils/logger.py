"""
src/utils/logger.py
-------------------

Logger utilitário com formatação consistente e nível configurável por variável
de ambiente (LOG_LEVEL). Use `get_logger(__name__)` nos módulos.

Exemplos:
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


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Retorna um logger configurado com handler único e formato consistente.

    Parâmetros
    ----------
    name : Optional[str]
        Nome do logger (geralmente __name__). Se None, usa "app".

    Retorno
    -------
    logging.Logger
        Instância pronta para uso.
    """
    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    logger = logging.getLogger(name or "app")

    # Evita adicionar múltiplos handlers ao reimportar o módulo (ex.: em testes).
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False

    logger.setLevel(level)
    return logger
