"""
Smoke test para BigQuery no projeto 'genai-rio'.

Objetivos
---------
- Validar autenticação (ADC) no Cloud Shell.
- Confirmar acesso ao dataset público 'datario'.
- Validar a consulta via DRY-RUN e depois executá-la.
- Exibir bytes estimados, tempo de execução e resultado.
- Fornecer diagnóstico rápido de conectividade e limites.

Configuração por ambiente (opcional)
------------------------------------
PROJECT_ID            (default: "genai-rio")
SMOKE_DATE            (default: "2024-11-28")   # valor para filtro em data_particao
BQ_LOCATION           (default: "US")
BQ_MAX_BYTES_BILLED   (default: 2GB)
BQ_QUERY_TIMEOUT      (default: 60)             # segundos
BQ_USE_BQSTORAGE      (default: "0")            # usa BigQuery Storage API se =1
"""

from __future__ import annotations

import os
import time
from typing import Optional

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, BadRequest


# Configuração por ambiente (valores com fallback)

PROJECT_ID = os.getenv("PROJECT_ID", "genai-rio")
SMOKE_DATE = os.getenv("SMOKE_DATE", "2024-11-28")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
BQ_MAX_BYTES_BILLED = int(os.getenv("BQ_MAX_BYTES_BILLED", str(2 * 10**9)))
BQ_QUERY_TIMEOUT = int(os.getenv("BQ_QUERY_TIMEOUT", "60"))
USE_BQSTORAGE = os.getenv("BQ_USE_BQSTORAGE", "0") == "1"

# Consulta simples para validar acesso: contagem de chamados em data específica
SQL = f"""
SELECT COUNT(1) AS n
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_particao = DATE '{SMOKE_DATE}'
"""

# Funções auxiliares


def _client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Cria cliente BigQuery com projeto e localização explícitos.
    Usa Application Default Credentials (ADC).
    """
    return bigquery.Client(project=project_id or PROJECT_ID, location=BQ_LOCATION)


def _dry_run(client: bigquery.Client, sql: str) -> int:
    """
    Executa DRY-RUN para validar sintaxe e estimar bytes processados.

    Returns
    -------
    int
        Número de bytes estimados pelo BigQuery.
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=True)
    job = client.query(sql, job_config=job_config)
    job.result()
    return int(job.total_bytes_processed or 0)


def _run(client: bigquery.Client, sql: str):
    """
    Executa a consulta real no BigQuery, com limites defensivos.

    - Respeita maximum_bytes_billed (teto de bytes).
    - Aplica timeout configurável.
    - Retorna DataFrame e se a resposta foi cacheada.

    Returns
    -------
    (DataFrame, cache_hit: bool|None)
    """
    job_config = bigquery.QueryJobConfig(
        dry_run=False,
        use_query_cache=True,
        maximum_bytes_billed=BQ_MAX_BYTES_BILLED,
        labels={"app": "genai-rio-agent", "env": os.getenv("ENV_LABEL", "dev")},
        priority=bigquery.QueryPriority.INTERACTIVE,
    )
    job = client.query(sql, job_config=job_config)
    result = job.result(timeout=BQ_QUERY_TIMEOUT)

    # BigQuery Storage opcional
    create_bqstorage = False
    if USE_BQSTORAGE:
        try:
            create_bqstorage = True
        except Exception:
            create_bqstorage = False

    df = result.to_dataframe(create_bqstorage_client=create_bqstorage)

    # Diagnóstico de cache
    cache_hit = None
    try:
        cache_hit = bool(job.cache_hit)
    except Exception:
        pass

    return df, cache_hit


def _bad_request_details(e: BadRequest) -> str:
    """
    Extrai detalhes adicionais de erro BadRequest (quando disponíveis).
    """
    base = getattr(e, "message", None) or str(e)
    try:
        errs = getattr(e, "errors", None)
        if isinstance(errs, list) and errs:
            parts = []
            for it in errs:
                msg = it.get("message")
                loc = it.get("location")
                parts.append(f"{msg} (em {loc})" if msg and loc else (msg or ""))
            parts = [p for p in parts if p]
            if parts:
                return base + " | detalhes: " + " | ".join(parts)
    except Exception:
        pass
    return base


# Entrypoint


def main() -> None:
    """
    Executa o smoke test: dry-run + execução real.
    Exibe métricas de custo/latência e valida resultado.
    """
    print(f"Projeto: {PROJECT_ID} | Location: {BQ_LOCATION}")
    print(f"data_particao alvo: {SMOKE_DATE}")
    print(
        f"maximum_bytes_billed: {BQ_MAX_BYTES_BILLED:,} | timeout: {BQ_QUERY_TIMEOUT}s | BQStorage: {USE_BQSTORAGE}"
    )
    print("Validando consulta (dry-run)...")

    try:
        client = _client()

        # Passo 1: validação via dry-run
        bytes_est = _dry_run(client, SQL)
        print(f"Dry-run OK | bytes estimados: {bytes_est:,}")

        # Passo 2: execução real
        print("Executando consulta...")
        t0 = time.time()
        df, cache_hit = _run(client, SQL)
        latency = (time.time() - t0) * 1000

        print(f"\nExecução OK | latência ~ {latency:.0f} ms | cache_hit={cache_hit}")
        print("Resultado:")
        print(df)

        # Verificação básica de integridade
        assert "n" in df.columns, "Coluna 'n' não encontrada no resultado."
        print("\nSmoke test OK ✅")

    except BadRequest as e:
        print("\nFalha de validação (BadRequest) no BigQuery.")
        print(_bad_request_details(e))
        raise
    except GoogleAPIError as e:
        print("\nFalha na API do Google BigQuery.")
        print(repr(e))
        raise
    except Exception as e:
        print("\nErro inesperado no smoke test.")
        print(repr(e))
        raise


if __name__ == "__main__":
    main()
