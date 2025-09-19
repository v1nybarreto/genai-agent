"""Utilitários de infraestrutura: BigQuery, schema, logger e LLM."""

from .bq import dry_run, execute  # BigQuery helpers (SELECT-only)
from .schema import get_table_schema  # Descoberta de schema via INFORMATION_SCHEMA
from .logger import get_logger  # Logger padronizado
from .llm import get_llm_response  # Camada fina de LLM (OpenAI)

__all__ = ["dry_run", "execute", "get_table_schema", "get_logger", "get_llm_response"]
