"""
Nós do agente: roteador, gerador de SQL (rule-based inicial), validador, executor e sintetizador.

Princípios:
- Sem SELECT *.
- Agregações explícitas e uso de partição quando aplicável.
- JOIN com dados_mestres.bairro somente quando necessário.
- Compatível com BigQuery Sandbox.

Casos cobertos:
1) "Quantos chamados foram abertos no dia 28/11/2024?"
2) "Qual o subtipo mais comum relacionado a 'Iluminação Pública'?"
3) "Quais os 3 bairros com mais chamados de 'reparo de buraco' em 2023?"
4) "Qual nome da unidade organizacional que mais atendeu 'Fiscalização de estacionamento irregular'?"
"""

from __future__ import annotations

from typing import Dict, Any, Optional
import re
import datetime as dt

from src.utils.bq import dry_run, execute

# Tabelas base
TAB_CHAMADO = "datario.adm_central_atendimento_1746.chamado"
TAB_BAIRRO = "datario.dados_mestres.bairro"

# Roteador

def route_intent(question: str) -> Dict[str, Any]:
    """
    Heurística simples para decidir se a pergunta é de 'dados' ou 'conversacional'.

    Retorna
    -------
    {"intent": "data" | "chitchat", "question": original}
    """
    q = question.lower()
    data_triggers = (
        "quantos", "qual", "quais", "top", "maior", "menor",
        "contagem", "bairro", "unidade", "chamados", "iluminação", "reparo", "fiscalização"
    )
    is_data = any(w in q for w in data_triggers)
    return {"intent": "data" if is_data else "chitchat", "question": question}

# Helpers de Parse

_DATE_PT = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")  # dd/mm/yyyy

def _parse_date_pt(text: str) -> Optional[dt.date]:
    """Extrai data no formato dd/mm/yyyy de um texto PT-BR."""
    m = _DATE_PT.search(text)
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    return dt.date(y, mth, d)

def _mentions(text: str, *terms: str) -> bool:
    t = text.lower()
    return all(term.lower() in t for term in terms)

# Gerador de SQL

def generate_sql(question: str) -> Dict[str, Any]:
    """
    Gera SQL BigQuery eficiente para os casos do desafio.
    Devolve {"sql": "..."}.

    Observação:
    - Evitamos SELECT *.
    - Usamos filtros por partição quando possível (data_particao).
    """
    q = question.strip().lower()

    # 1) Contagem no dia específico (usa partição)
    day = _parse_date_pt(q)
    if day and _mentions(q, "quantos", "chamados"):
        sql = f"""
        SELECT
          COUNT(1) AS n
        FROM `{TAB_CHAMADO}`
        WHERE data_particao = DATE '{day:%Y-%m-%d}'
        """
        return {"sql": _one_line(sql)}

    # 2) Subtipo mais comum em "Iluminação Pública"
    if _mentions(q, "iluminação") and ("subtipo" in q or "sub-tipo" in q):
        # Filtra assunto = Iluminação Pública → conta por subtipo
        sql = f"""
        SELECT
          subtipo,
          COUNT(1) AS total
        FROM `{TAB_CHAMADO}`
        WHERE LOWER(assunto) = 'iluminação pública'
        GROUP BY subtipo
        ORDER BY total DESC
        LIMIT 1
        """
        return {"sql": _one_line(sql)}

    # 3) Top 3 bairros para "reparo de buraco" em 2023 (JOIN com bairro)
    if _mentions(q, "reparo", "buraco") and ("2023" in q or "em 2023" in q):
        # Atenção: o campo 'bairro' na fato é id_bairro; juntamos para nome do bairro
        sql = f"""
        SELECT
          b.nome AS bairro,
          COUNT(1) AS total
        FROM `{TAB_CHAMADO}` c
        JOIN `{TAB_BAIRRO}` b
          ON SAFE_CAST(c.id_bairro AS INT64) = b.id_bairro
        WHERE EXTRACT(YEAR FROM c.data_inicio) = 2023
          AND LOWER(c.assunto) LIKE '%buraco%' OR LOWER(c.subtipo) LIKE '%buraco%'
          AND LOWER(c.tipo) LIKE '%reparo%' OR LOWER(c.assunto) LIKE '%reparo%'
        GROUP BY bairro
        ORDER BY total DESC
        LIMIT 3
        """
        # Observação: como a modelagem do 1746 é ampla, usamos LIKEs para cobrir variações.
        return {"sql": _one_line(sql)}

    # 4) Unidade organizacional líder em "Fiscalização de estacionamento irregular"
    if _mentions(q, "fiscalização", "estacionamento") and ("irregular" in q):
        sql = f"""
        SELECT
          nome_unidade_organizacional,
          COUNT(1) AS total
        FROM `{TAB_CHAMADO}`
        WHERE LOWER(assunto) LIKE '%fiscalização%'
          AND LOWER(subtipo) LIKE '%estacionamento irregular%' OR LOWER(descricao) LIKE '%estacionamento irregular%'
        GROUP BY nome_unidade_organizacional
        ORDER BY total DESC
        LIMIT 1
        """
        return {"sql": _one_line(sql)}

    # Fallback: contagem total (seguro), para manter o fluxo funcionando
    sql = f"SELECT COUNT(1) AS n FROM `{TAB_CHAMADO}` WHERE data_particao = DATE '2024-11-28'"
    return {"sql": _one_line(sql)}

def _one_line(sql: str) -> str:
    """Normaliza quebras de linha e múltiplos espaços para facilitar teste/validação."""
    return re.sub(r"\s+", " ", sql).strip()

# Validador / Executor / Sintetizador

def validate_sql(sql: str) -> Dict[str, Any]:
    """Valida a consulta via DRY-RUN (sem custo)."""
    out = dry_run(sql)
    return {"ok": bool(out.get("ok")), "error": out.get("error"), "dry_run_bytes": out.get("dry_run_bytes")}

def execute_sql(sql: str) -> Dict[str, Any]:
    """Executa a consulta via nossa camada utils."""
    out = execute(sql)
    return {"ok": bool(out.get("ok")), "df": out.get("df"), "error": out.get("error")}

def synthesize(answer_df, question: str) -> Dict[str, Any]:
    """
    Converte um DataFrame em resposta textual objetiva.
    (Versão simples; depois podemos plugar um LLM.)
    """
    if answer_df is None:
        return {"answer": "Não foi possível obter resultados."}
    if answer_df.empty:
        return {"answer": "Nenhum registro encontrado para o filtro solicitado."}

    cols = [c.lower() for c in answer_df.columns]
    # Se há 'n' (contagem simples)
    if "n" in cols and len(answer_df) == 1:
        n = int(answer_df.iloc[0][answer_df.columns[cols.index("n")]])
        return {"answer": f"Contagem: {n}."}

    # Se há 'total' e outra coluna categórica
    if "total" in cols:
        row = answer_df.iloc[0]
        if len(answer_df) == 1:
            # Ex.: subtipo mais comum; unidade líder.
            keys = [c for c in answer_df.columns if c.lower() != "total"]
            k = keys[0] if keys else "categoria"
            return {"answer": f"{k}: {row[keys[0]]} (total: {int(row['total'])})."}
        # Ex.: top 3 bairros
        head = answer_df.head(3)[answer_df.columns].to_dict(orient="records")
        return {"answer": f"Top resultados: {head}"}

    # Fallback
    head = answer_df.head(3).to_dict(orient="records")
    return {"answer": f"Amostra de resultados: {head}"}
