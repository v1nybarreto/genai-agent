"""
Utilidades para BigQuery usadas pelo agente.

Objetivos:
- Fornecer cliente BigQuery a partir de ADC (Cloud Shell).
- Validar consultas com DRY-RUN antes de executar.
- Executar SELECTs com retorno em pandas.DataFrame.
- Bloquear comandos perigosos (DML/DDL) por padrão.
- Tratar erros de forma controlada e com mensagens claras.

Adequação ao desafio:
- Mantém consultas eficientes e seguras.
- Evita SELECT * (essa regra será aplicada no gerador de SQL).
- Usa partições quando aplicável (no nível do SQL gerado).
"""

from __future__ import annotations

from typing import Optional, TypedDict
import os
import re
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, BadRequest


# Tipagem do retorno para chamadas utilitárias

class QueryOutcome(TypedDict, total=False):
    ok: bool                    # True se sucesso; False se falha
    dry_run_bytes: int | None   # bytes estimados pelo dry-run
    df: Optional[pd.DataFrame]  # resultado da execução (quando aplicável)
    error: Optional[str]        # mensagem de erro (quando aplicável)


# Configuração básica

DEFAULT_PROJECT = os.getenv("PROJECT_ID", "genai-rio")

# Comandos DML/DDL proibidos por segurança (o agente só deve rodar SELECT)
_FORBIDDEN_PAT = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|CREATE|DROP|ALTER)\b",
    flags=re.IGNORECASE,
)


def get_bq_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Retorna um cliente BigQuery usando Application Default Credentials (ADC).
    No Cloud Shell, a ADC já está configurada.

    Parâmetros
    ----------
    project_id : str | None
        ID do projeto GCP. Se None, usa variável de ambiente PROJECT_ID
        (ou 'genai-rio' como fallback).

    Retorno
    -------
    bigquery.Client
    """
    return bigquery.Client(project=project_id or DEFAULT_PROJECT)


def is_select_only(sql: str) -> bool:
    """
    Verifica de forma simples se a consulta parece ser apenas SELECT.

    Observação: não substitui um parser SQL completo, mas evita DML/DDL comuns.
    """
    if _FORBIDDEN_PAT.search(sql):
        return False
    # Heurística adicional: começa com SELECT (ignorando espaços/comentários)
    stripped = re.sub(r"(^|\n)\s*--.*", "", sql).lstrip()
    return stripped.upper().startswith("SELECT")


def dry_run(sql: str, project_id: Optional[str] = None) -> QueryOutcome:
    """
    Executa um DRY-RUN no BigQuery para validar sintaxe e estimar bytes processados.

    Retorno
    -------
    QueryOutcome:
        ok=True se a validação passou; `dry_run_bytes` com a estimativa.
        ok=False com `error` se falhar.
    """
    if not is_select_only(sql):
        return {"ok": False, "error": "Apenas SELECT é permitido (DML/DDL bloqueados)."}

    client = get_bq_client(project_id)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=True)

    try:
        job = client.query(sql, job_config=job_config)
        # O acesso a result() dispara a validação no modo dry-run
        job.result()
        return {"ok": True, "dry_run_bytes": job.total_bytes_processed}
    except BadRequest as e:
        # Erros SQL (sintaxe, campos inexistentes, etc.)
        return {"ok": False, "error": f"Erro de validação (BadRequest): {e.message}"}
    except GoogleAPIError as e:
        return {"ok": False, "error": f"Erro na API do BigQuery: {repr(e)}"}
    except Exception as e:
        return {"ok": False, "error": f"Erro inesperado no dry-run: {repr(e)}"}


def execute(sql: str, project_id: Optional[str] = None) -> QueryOutcome:
    """
    Executa uma consulta SELECT no BigQuery e retorna um DataFrame.

    Fluxo:
    1) Dry-run para validação/custo.
    2) Execução real se dry-run passar.

    Retorno
    -------
    QueryOutcome:
        ok=True e `df` preenchido em sucesso; caso contrário, `error` preenchido.
    """
    # 1) validação
    dv = dry_run(sql, project_id=project_id)
    if not dv.get("ok"):
        return dv  # retorna o erro do dry-run

    client = get_bq_client(project_id)
    job_config = bigquery.QueryJobConfig(dry_run=False, use_query_cache=True)

    try:
        df = client.query(sql, job_config=job_config).result().to_dataframe()
        return {"ok": True, "dry_run_bytes": dv.get("dry_run_bytes"), "df": df}
    except GoogleAPIError as e:
        return {"ok": False, "error": f"Erro na API do BigQuery: {repr(e)}"}
    except Exception as e:
        return {"ok": False, "error": f"Erro inesperado na execução: {repr(e)}"}