"""
Utilidades para BigQuery usadas pelo agente.

Objetivos
---------
- Fornecer cliente BigQuery a partir de ADC (Cloud Shell).
- Validar consultas com DRY-RUN antes de executar.
- Executar SELECTs com retorno em pandas.DataFrame.
- Bloquear comandos perigosos (DML/DDL) e múltiplas sentenças.
- Tratar erros de forma controlada e com mensagens claras.

Adequação ao desafio
--------------------
- Mantém consultas eficientes e seguras (sem DML/DDL).
- Evita `SELECT *` (regra reforçada no gerador de SQL; aqui há checagem leve).
- Usa partições quando aplicável (no SQL gerado).
- Limita custo via `maximum_bytes_billed` e DRY-RUN + timeout.
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
    ok: bool  # True se sucesso; False se falha
    dry_run_bytes: int | None  # bytes estimados pelo dry-run
    df: Optional[pd.DataFrame]  # resultado da execução (quando aplicável)
    error: Optional[str]  # mensagem de erro (quando aplicável)


# Configuração básica por ambiente

DEFAULT_PROJECT = os.getenv("PROJECT_ID", "genai-rio")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Limite "defensivo" de bytes faturáveis em execução real
MAX_BYTES_BILLED = int(os.getenv("BQ_MAX_BYTES_BILLED", str(2 * 10**9)))

# Timeout (segundos) para aguardar conclusão de uma query
QUERY_TIMEOUT = int(os.getenv("BQ_QUERY_TIMEOUT", "60"))

# Rótulos de auditoria (visíveis no Job do BigQuery)
JOB_LABELS = {
    "app": os.getenv("APP_LABEL", "genai-rio-agent"),
    "env": os.getenv("ENV_LABEL", "dev"),
}

# Permite usar BigQuery Storage API para to_dataframe
USE_BQSTORAGE = os.getenv("BQ_USE_BQSTORAGE", "0") == "1"

# Comandos DML/DDL/administrativos proibidos por segurança
_FORBIDDEN_PAT = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|CREATE|DROP|ALTER|GRANT|REVOKE|"
    r"BEGIN|COMMIT|ROLLBACK|CALL|EXECUTE\s+IMMEDIATE|EXPORT|LOAD\s+DATA)\b",
    flags=re.IGNORECASE,
)

# Comentários SQL (linha única) e múltiplas sentenças
_SQL_LINE_COMMENTS = re.compile(r"(^|\n)\s*--.*")
# Qualquer ponto-e-vírgula (;) fora de comentários indica múltiplas sentenças.
_SQL_ANY_SEMICOLON = re.compile(r";")

# SELECT * (checagem leve; removidos comentários)
_SQL_SELECT_STAR = re.compile(
    r"^\s*(WITH\b.*?\bSELECT\b|SELECT\b).*?\*\s", re.IGNORECASE | re.DOTALL
)


# Cliente


def get_bq_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Cria um cliente BigQuery usando Application Default Credentials (ADC).

    Parameters
    ----------
    project_id : str | None
        ID do projeto GCP. Se None, usa PROJECT_ID (ou 'genai-rio' como fallback).

    Returns
    -------
    google.cloud.bigquery.Client
    """
    return bigquery.Client(project=project_id or DEFAULT_PROJECT, location=BQ_LOCATION)


# Helpers


def _strip_comments(sql: str) -> str:
    """Remove comentários de linha ('-- ...') para análise estática simples."""
    return _SQL_LINE_COMMENTS.sub(lambda m: m.group(1) if m.group(1) else "", sql or "")


def _normalize_sql(sql: str) -> str:
    """Remove comentários e normaliza espaços para verificações heurísticas."""
    s = _strip_comments(sql)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _err_from_badrequest(e: BadRequest) -> str:
    """Extrai mensagens detalhadas de BadRequest (quando disponíveis)."""
    # Alguns BadRequest têm .errors
    parts = []
    try:
        errs = getattr(e, "errors", None)
        if isinstance(errs, list):
            for item in errs:
                msg = item.get("message")
                loc = item.get("location")
                if msg and loc:
                    parts.append(f"{msg} (em {loc})")
                elif msg:
                    parts.append(msg)
    except Exception:
        pass
    base = getattr(e, "message", None) or str(e)
    if parts:
        return base + " | detalhes: " + " | ".join(parts)
    return base


# Validadores de segurança


def is_select_only(sql: str) -> bool:
    """
    Verifica de forma pragmática se a consulta parece ser apenas SELECT e de sentença única.

    Regras:
    - Bloqueia DML/DDL/administrativo (INSERT/UPDATE/.../EXECUTE IMMEDIATE etc.).
    - Bloqueia múltiplas sentenças (qualquer ';', mesmo no final).
    - Exige que o texto (sem comentários) comece com `SELECT` ou `WITH` (CTE).

    Observação: não substitui um parser SQL completo, mas é suficiente para o agente.
    """
    if not sql or not sql.strip():
        return False

    stripped = _normalize_sql(sql)

    # Checagem de múltiplas sentenças
    if _SQL_ANY_SEMICOLON.search(stripped):
        return False

    # Checagem de DML/DDL
    if _FORBIDDEN_PAT.search(stripped):
        return False

    # Heurística: começa com SELECT ou WITH
    up = stripped.upper()
    return up.startswith("SELECT") or up.startswith("WITH")


def has_select_star(sql: str) -> bool:
    """
    Checagem leve para 'SELECT *' (após remover comentários).
    Evita varreduras desnecessárias e cumpre o requisito do desafio.
    """
    stripped = _normalize_sql(sql)
    return bool(_SQL_SELECT_STAR.search(stripped))


# DRY-RUN


def dry_run(sql: str, project_id: Optional[str] = None) -> QueryOutcome:
    """
    Executa um DRY-RUN no BigQuery para validar sintaxe e estimar bytes processados.

    Returns
    -------
    QueryOutcome
        ok=True se a validação passou; `dry_run_bytes` com a estimativa.
        ok=False com `error` se falhar.
    """
    if not (sql or "").strip():
        return {
            "ok": False,
            "error": "SQL vazio para validação.",
            "dry_run_bytes": None,
        }

    if not is_select_only(sql):
        return {
            "ok": False,
            "error": "Apenas SELECT (ou WITH ... SELECT) é permitido; DML/DDL ou múltiplas sentenças são bloqueadas.",
        }

    if has_select_star(sql):
        return {
            "ok": False,
            "error": "Uso de 'SELECT *' bloqueado. Projete colunas explicitamente.",
            "dry_run_bytes": None,
        }

    client = get_bq_client(project_id)
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=True,
        labels=JOB_LABELS,
    )

    try:
        job = client.query(sql, job_config=job_config)
        # job.result() dispara a validação no modo dry-run
        job.result()
        return {"ok": True, "dry_run_bytes": job.total_bytes_processed}
    except BadRequest as e:
        # Erros SQL
        return {
            "ok": False,
            "error": f"Erro de validação (BadRequest): {_err_from_badrequest(e)}",
        }
    except GoogleAPIError as e:
        return {"ok": False, "error": f"Erro na API do BigQuery: {repr(e)}"}
    except Exception as e:
        return {"ok": False, "error": f"Erro inesperado no dry-run: {repr(e)}"}


# Execução


def execute(sql: str, project_id: Optional[str] = None) -> QueryOutcome:
    """
    Executa uma consulta SELECT no BigQuery e retorna um DataFrame.

    Fluxo
    -----
    1) Dry-run para validação/custo.
    2) Execução real se dry-run passar e se `maximum_bytes_billed` permitir.

    Returns
    -------
    QueryOutcome
        ok=True e `df` preenchido em sucesso; caso contrário, `error` preenchido.
    """
    if not (sql or "").strip():
        return {"ok": False, "error": "SQL vazio no executor.", "df": None}

    # 1) validação (dry-run)
    dv = dry_run(sql, project_id=project_id)
    if not dv.get("ok"):
        # Propaga o erro do dry-run
        return {
            "ok": False,
            "error": dv.get("error"),
            "df": None,
            "dry_run_bytes": dv.get("dry_run_bytes"),
        }

    # Bloqueio por custo estimado
    est_bytes = dv.get("dry_run_bytes") or 0
    if MAX_BYTES_BILLED and est_bytes and est_bytes > MAX_BYTES_BILLED:
        return {
            "ok": False,
            "error": (
                f"Custo estimado alto para o Sandbox ({est_bytes} bytes). "
                f"Refine filtros (datas/colunas) ou reduza escopo da consulta."
            ),
            "df": None,
            "dry_run_bytes": est_bytes,
        }

    client = get_bq_client(project_id)
    job_config = bigquery.QueryJobConfig(
        dry_run=False,
        use_query_cache=True,
        # Limita o job a um teto de bytes faturáveis
        maximum_bytes_billed=MAX_BYTES_BILLED if MAX_BYTES_BILLED else None,
        priority=bigquery.QueryPriority.INTERACTIVE,
        labels=JOB_LABELS,
    )

    try:
        job = client.query(sql, job_config=job_config)
        # Aguarda com timeout defensivo para não travar o agente
        result = job.result(timeout=QUERY_TIMEOUT)

        # Converte para DataFrame
        create_bqstorage = False
        if USE_BQSTORAGE:
            try:
                # Se a lib estiver instalada, este import funciona.
                create_bqstorage = True
            except Exception:
                create_bqstorage = False

        df = result.to_dataframe(create_bqstorage_client=create_bqstorage)
        return {"ok": True, "dry_run_bytes": est_bytes, "df": df}
    except BadRequest as e:
        return {
            "ok": False,
            "error": f"Erro de execução (BadRequest): {_err_from_badrequest(e)}",
            "df": None,
        }
    except GoogleAPIError as e:
        return {"ok": False, "error": f"Erro na API do BigQuery: {repr(e)}", "df": None}
    except Exception as e:
        # Inclui possibilidade de Timeout
        return {
            "ok": False,
            "error": f"Erro inesperado na execução: {repr(e)}",
            "df": None,
        }
