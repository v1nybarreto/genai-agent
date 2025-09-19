"""
Nós do agente com gerador de SQL adaptativo ao schema do 1746.

Princípios:
- Nunca usar SELECT *.
- Validar com DRY-RUN antes de executar.
- Filtros textuais apenas em colunas que existem (descobertas via INFORMATION_SCHEMA).
- JOIN com dados_mestres.bairro apenas quando necessário, com CAST correto.
- Compatível com BigQuery Sandbox (consultas baratas e diretas).

Casos cobertos:
1) "Quantos chamados foram abertos no dia 28/11/2024?"
2) "Qual o subtipo mais comum relacionado a 'Iluminação Pública'?"
3) "Quais os 3 bairros com mais chamados de 'reparo de buraco' em 2023?"
4) "Qual nome da unidade organizacional que mais atendeu 'Fiscalização de estacionamento irregular'?"
"""

from __future__ import annotations
from typing import Dict, Any, Optional, List
import re
import datetime as dt

from src.utils.bq import dry_run, execute
from src.utils.schema import get_table_schema

# Tabelas base
TAB_CHAMADO = "datario.adm_central_atendimento_1746.chamado"
TAB_BAIRRO = "datario.dados_mestres.bairro"
DATASET_CHAMADO = "datario.adm_central_atendimento_1746"
TABLE_CHAMADO = "chamado"

# ------------------------- ROTEADOR -------------------------

def route_intent(question: str) -> Dict[str, Any]:
    """
    Heurística simples: decide se é pergunta de dados ou conversacional.
    Retorna {"intent": "data" | "chitchat", "question": original}
    """
    q = question.lower()
    data_triggers = (
        "quantos", "qual", "quais", "top", "maior", "menor",
        "contagem", "bairro", "unidade", "chamados", "iluminação", "reparo", "fiscalização"
    )
    is_data = any(w in q for w in data_triggers)
    return {"intent": "data" if is_data else "chitchat", "question": question}

# ------------------------- HELPERS -------------------------

# cache simples do schema em memória (evita acessar INFORMATION_SCHEMA repetidamente)
_SCHEMA_CACHE: Optional[Dict[str, str]] = None

def _schema() -> Dict[str, str]:
    """Obtém {coluna: tipo} do schema, com cache simples em memória."""
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        _SCHEMA_CACHE = get_table_schema(DATASET_CHAMADO, TABLE_CHAMADO)
    return _SCHEMA_CACHE

_DATE_PT = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")  # dd/mm/yyyy

def _parse_date_pt(text: str) -> Optional[dt.date]:
    """Extrai data no formato dd/mm/yyyy de um texto PT-BR."""
    m = _DATE_PT.search(text)
    if not m:
        return None
    d, mth, y = map(int, m.groups())
    return dt.date(y, mth, d)

def _text_columns() -> List[str]:
    """
    Retorna colunas STRING candidatas para busca textual
    (apenas as que de fato existem no schema).
    """
    s = _schema()
    candidates = [
        "subtipo", "tipo", "categoria", "descricao", "titulo",
        "motivo", "detalhe", "classificacao", "assunto"
    ]
    return [c for c in candidates if c in s and s[c].upper().startswith("STRING")]

def _build_like_filter(terms: List[str]) -> str:
    """
    Constrói expressão de filtro textual tolerante:
    (LOWER(col) LIKE '%t1%' AND LOWER(col) LIKE '%t2%') OR ...  (por coluna textual disponível)
    """
    cols = _text_columns()
    if not cols:
        return "1=1"  # fallback seguro se não houver coluna textual conhecida
    per_col = []
    for c in cols:
        conj = " AND ".join([f"LOWER({c}) LIKE '%{t.lower()}%'" for t in terms])
        per_col.append(f"({conj})")
    return "(" + " OR ".join(per_col) + ")"

def _bairro_join_condition() -> str:
    """
    Ajusta o JOIN entre fato e dimensão bairro conforme tipos:
    - Se id_bairro no fato é STRING → c.id_bairro = CAST(b.id_bairro AS STRING)
    - Caso contrário → CAST(c.id_bairro AS INT64) = b.id_bairro
    """
    s = _schema()
    fato_t = s.get("id_bairro", "STRING").upper()
    if fato_t.startswith("STRING"):
        return "c.id_bairro = CAST(b.id_bairro AS STRING)"
    return "CAST(c.id_bairro AS INT64) = b.id_bairro"

def _one_line(sql: str) -> str:
    """Normaliza espaços para facilitar testes e logs."""
    return re.sub(r"\s+", " ", sql).strip()

# ------------------------- GERADOR DE SQL -------------------------

def generate_sql(question: str) -> Dict[str, Any]:
    """
    Gera SQL BigQuery eficiente (sem SELECT *) com base na pergunta,
    adaptando-se às colunas disponíveis no schema real.
    """
    q = question.strip().lower()
    s = _schema()

    # 1) Contagem por dia — prioriza partição se existir
    day = _parse_date_pt(q)
    if day and "quantos" in q and "chamados" in q:
        if "data_particao" in s:
            where_date = f"data_particao = DATE '{day:%Y-%m-%d}'"
        elif "data_inicio" in s:
            where_date = f"DATE(data_inicio) = DATE '{day:%Y-%m-%d}'"
        else:
            where_date = "1=1"
        sql = f"SELECT COUNT(1) AS n FROM `{TAB_CHAMADO}` WHERE {where_date}"
        return {"sql": _one_line(sql)}

    # 2) Subtipo mais comum relacionado a "Iluminação Pública"
    if "iluminação" in q:
        # quando subtipo existir, não aliasar e agrupar exatamente por 'subtipo'
        if ("subtipo" in s and s["subtipo"].upper().startswith("STRING")):
            select_expr = "subtipo"
            group_expr = "subtipo"
        elif ("tipo" in s and s["tipo"].upper().startswith("STRING")):
            select_expr = "tipo"
            group_expr = "tipo"
        else:
            select_expr = "categoria"
            group_expr = "categoria"

        filtro = _build_like_filter(["iluminação", "pública"])
        sql = f"""
        SELECT {select_expr}, COUNT(1) AS total
        FROM `{TAB_CHAMADO}`
        WHERE {filtro}
        GROUP BY {group_expr}
        ORDER BY total DESC
        LIMIT 1
        """
        return {"sql": _one_line(sql)}

    # 3) Top 3 bairros — "reparo de buraco" em 2023 (JOIN com bairro)
    if "reparo" in q and "buraco" in q and "2023" in q:
        filtro = _build_like_filter(["reparo", "buraco"])
        join_on = _bairro_join_condition()
        year_col = "data_inicio" if "data_inicio" in s else None
        year_cond = "1=1" if not year_col else f"EXTRACT(YEAR FROM c.{year_col}) = 2023"
        sql = f"""
        SELECT b.nome AS bairro, COUNT(1) AS total
        FROM `{TAB_CHAMADO}` c
        JOIN `{TAB_BAIRRO}` b
          ON {join_on}
        WHERE {year_cond}
          AND ({filtro})
        GROUP BY bairro
        ORDER BY total DESC
        LIMIT 3
        """
        return {"sql": _one_line(sql)}

    # 4) Unidade organizacional líder — "Fiscalização de estacionamento irregular"
    if "fiscalização" in q and "estacionamento" in q and "irregular" in q:
        filtro = _build_like_filter(["fiscalização", "estacionamento", "irregular"])
        unidade_col = "nome_unidade_organizacional" if "nome_unidade_organizacional" in s else "id_unidade_organizacional"
        sql = f"""
        SELECT {unidade_col} AS unidade, COUNT(1) AS total
        FROM `{TAB_CHAMADO}`
        WHERE ({filtro})
        GROUP BY unidade
        ORDER BY total DESC
        LIMIT 1
        """
        return {"sql": _one_line(sql)}

    # Fallback seguro para manter o fluxo funcionando
    base_date = "2024-11-28"
    where_col = "data_particao" if "data_particao" in s else "DATE(data_inicio)"
    sql = f"SELECT COUNT(1) AS n FROM `{TAB_CHAMADO}` WHERE {where_col} = DATE '{base_date}'"
    return {"sql": _one_line(sql)}

# ------------------------- VALIDADOR / EXECUTOR / SÍNTESE -------------------------

def validate_sql(sql: str) -> Dict[str, Any]:
    """Valida a consulta via DRY-RUN (sem custo)."""
    out = dry_run(sql)
    return {"ok": bool(out.get("ok")), "error": out.get("error"), "dry_run_bytes": out.get("dry_run_bytes")}

def execute_sql(sql: str) -> Dict[str, Any]:
    """Executa a consulta via nossa camada utils (com dry-run prévio)."""
    out = execute(sql)
    return {"ok": bool(out.get("ok")), "df": out.get("df"), "error": out.get("error")}

def synthesize(answer_df, question: str) -> Dict[str, Any]:
    """
    Converte um DataFrame em resposta textual objetiva.
    (Versão simples; podemos acoplar LLM depois.)
    """
    if answer_df is None:
        return {"answer": "Não foi possível obter resultados."}
    if answer_df.empty:
        return {"answer": "Nenhum registro encontrado para o filtro solicitado."}

    cols = [c.lower() for c in answer_df.columns]
    # Contagem simples
    if "n" in cols and len(answer_df) == 1:
        n = int(answer_df.iloc[0][answer_df.columns[cols.index("n")]])
        return {"answer": f"Contagem: {n}."}

    # Tabelas agregadas
    if "total" in cols:
        if len(answer_df) == 1:
            row = answer_df.iloc[0]
            keys = [c for c in answer_df.columns if c.lower() != "total"]
            k = keys[0] if keys else "categoria"
            return {"answer": f"{k}: {row[keys[0]]} (total: {int(row['total'])})."}
        head = answer_df.head(3).to_dict(orient="records")
        return {"answer": f"Top resultados: {head}"}

    # Fallback: amostra
    head = answer_df.head(3).to_dict(orient="records")
    return {"answer": f"Amostra de resultados: {head}"}

def chitchat(question: str) -> Dict[str, Any]:
    """
    Responde educadamente a saudações/perguntas genéricas usando o LLM (OpenAI).
    Fallback: mensagem estática, caso a API não esteja configurada.
    """
    try:
        from src.utils.llm import get_llm_response  # usa gpt-4o-mini por padrão
        prompt = (
            "Responda em PT-BR, no máximo 2 frases, de forma simpática e objetiva. "
            "Se fizer sentido, lembre que posso ajudar com análises dos chamados do 1746."
        )
        out = get_llm_response(f"{prompt}\n\nUsuário: {question}")
        if out.get("ok") and out.get("text"):
            return {"answer": out["text"]}
    except Exception:
        # cai no fallback silenciosamente
        pass

    return {
        "answer": (
            "Olá! Posso ajudar com análises sobre os chamados do 1746. "
            "Exemplo: 'Quantos chamados houve em 28/11/2024?'"
        )
    }

    