"""
Descoberta de schema no BigQuery (tipos por coluna).

Função principal:
- get_table_schema(dataset: str, table: str) -> Dict[str, str]
  Retorna um dicionário {coluna: tipo} usando INFORMATION_SCHEMA.

Uso:
    get_table_schema("datario.adm_central_atendimento_1746", "chamado")
"""

from __future__ import annotations
from typing import Dict
from src.utils.bq import execute


def get_table_schema(dataset: str, table: str) -> Dict[str, str]:
    """
    Retorna {coluna: tipo} de dataset.table via INFORMATION_SCHEMA.

    Parameters
    ----------
    dataset : str
        Nome no formato "project.dataset" ou "dataset" já resolvido no projeto atual.
    table : str
        Nome da tabela (sem qualificador).

    Returns
    -------
    Dict[str, str]
        Mapeamento coluna -> tipo BigQuery (em caixa alta).
    """
    sql = f"""
    SELECT column_name, data_type
    FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    """
    out = execute(sql)
    if not out.get("ok"):
        raise RuntimeError(f"Falha ao obter schema: {out.get('error')}")
    df = out["df"]
    return {str(r["column_name"]): str(r["data_type"]).upper() for _, r in df.iterrows()}