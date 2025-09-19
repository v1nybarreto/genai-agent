"""
Descoberta de schema no BigQuery (tipos por coluna).

Função principal
----------------
- get_table_schema(dataset: str, table: str) -> Dict[str, str]
  Retorna um dicionário {coluna: tipo} consultando INFORMATION_SCHEMA.COLUMNS.

Exemplo:
    get_table_schema("datario.adm_central_atendimento_1746", "chamado")
"""

from __future__ import annotations

from typing import Dict
import re
from functools import lru_cache

from src.utils.bq import execute

# Permitimos apenas caracteres seguros em identificadores de BQ
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.$]+$")


def _strip_backticks(name: str) -> str:
    """Remove acentos graves (backticks) de um identificador, se existirem."""
    return (name or "").strip().strip("`").strip()


def _validate_identifier(name: str, kind: str) -> str:
    """
    Sanitiza e valida um identificador simples (dataset ou tabela).

    - Remove backticks de borda.
    - Garante apenas caracteres seguros (evita injeção em f-strings).
    """
    n = _strip_backticks(name)
    if not n or not _SAFE_ID.match(n):
        raise ValueError(f"Identificador {kind!r} inválido: {name!r}")
    return n


@lru_cache(maxsize=64)
def get_table_schema(dataset: str, table: str) -> Dict[str, str]:
    """
    Retorna {coluna: tipo} de dataset.table via INFORMATION_SCHEMA.

    Parameters
    ----------
    dataset : str
        Nome no formato "project.dataset" ou apenas "dataset" (será resolvido no projeto atual).
        Caracteres permitidos: letras/dígitos/underscore/ponto/cifrão.
    table : str
        Nome da tabela (sem qualificador). Caracteres permitidos: letras/dígitos/underscore/ponto/cifrão.

    Returns
    -------
    Dict[str, str]
        Mapeamento coluna -> tipo BigQuery (UPPERCASE).
        Lança ValueError para identificadores inválidos e RuntimeError em falha de consulta.
    """
    ds = _validate_identifier(dataset, "dataset")
    tb = _validate_identifier(table, "table")

    # INFORMATION_SCHEMA é resolvido por dataset
    sql = f"""
        SELECT column_name, data_type
        FROM `{ds}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{tb}'
    """

    out = execute(sql)
    if not out.get("ok"):
        raise RuntimeError(f"Falha ao obter schema de {ds}.{tb}: {out.get('error')}")

    df = out.get("df")
    if df is None or df.empty:
        raise RuntimeError(
            f"Nenhuma coluna encontrada em {ds}.{tb}. Verifique se a tabela existe."
        )

    # Normaliza para dict coluna
    return {
        str(row["column_name"]): str(row["data_type"]).upper()
        for _, row in df.iterrows()
    }
