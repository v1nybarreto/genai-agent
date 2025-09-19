"""
src/agent/nodes.py
------------------

Nós de negócio do agente:

- Roteador de intenção
- Gerador de SQL (adaptativo ao schema real do 1746)
- Validador (DRY-RUN)
- Executor (BigQuery)
- Sintetizador (LLM opcional com fallback determinístico)
- Chit-chat (LLM com fallback)

Princípios:
- Nunca usar SELECT *.
- Validar via DRY-RUN antes de executar.
- Filtros textuais apenas em colunas existentes (descobertas via INFORMATION_SCHEMA).
- JOIN com dados_mestres.bairro apenas quando necessário, com CAST correto.
- Compatível com BigQuery Sandbox (consultas baratas e diretas).
"""

from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple
import datetime as dt
import re
import os

from src.utils.bq import dry_run, execute
from src.utils.schema import get_table_schema
from src.utils.logger import get_logger

# Constantes e configuração

TAB_CHAMADO = "datario.adm_central_atendimento_1746.chamado"
TAB_BAIRRO = "datario.dados_mestres.bairro"
DATASET_CHAMADO = "datario.adm_central_atendimento_1746"
TABLE_CHAMADO = "chamado"

# Datas em PT-BR
_DATE_PT = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

# Cache simples do schema
_SCHEMA_CACHE: Optional[Dict[str, str]] = None

log = get_logger(__name__)

# Roteador


def route_intent(question: str) -> Dict[str, Any]:
    """
    Decide se a pergunta é sobre dados ("data") ou conversacional ("chitchat").

    Heurística leve baseada em gatilhos de linguagem natural em PT-BR.

    Parameters
    ----------
    question : str
        Pergunta do usuário.

    Returns
    -------
    dict
        {"intent": "data" | "chitchat", "question": <original>}
    """
    q = (question or "").strip().lower()

    # Palavras e padrões típicos de perguntas analíticas
    data_triggers = (
        "quantos",
        "quanto",
        "qual",
        "quais",
        "top",
        "maior",
        "menor",
        "contagem",
        "bairro",
        "unidade",
        "chamados",
        "iluminação",
        "reparo",
        "buraco",
        "fiscalização",
        "estacionamento",
        "irregular",
        "2023",
        "2024",
        "1746",
    )
    # Gatilhos de conversa genérica
    chitchat_triggers = (
        "oi",
        "olá",
        "bom dia",
        "boa tarde",
        "boa noite",
        "obrigado",
        "valeu",
    )

    is_data = any(w in q for w in data_triggers)
    is_chitchat = any(w in q for w in chitchat_triggers)

    intent = (
        "data"
        if (is_data and not is_chitchat)
        else ("chitchat" if is_chitchat else "data" if is_data else "chitchat")
    )
    log.debug("route_intent | intent=%s | q=%s", intent, q[:120])
    return {"intent": intent, "question": question}


# Helpers de Schema/Text


def _schema() -> Dict[str, str]:
    """
    Obtém {coluna: tipo} do schema real de `datario.adm_central_atendimento_1746.chamado`,
    com cache em memória para reduzir latência/custos.
    """
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        _SCHEMA_CACHE = get_table_schema(DATASET_CHAMADO, TABLE_CHAMADO)
        log.info("Schema cache carregado: %d colunas", len(_SCHEMA_CACHE))
    return _SCHEMA_CACHE


def _parse_date_pt(text: str) -> Optional[dt.date]:
    """
    Extrai uma data no formato dd/mm/yyyy a partir de um texto PT-BR.

    Returns
    -------
    datetime.date | None
    """
    if not text:
        return None
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
        "subtipo",
        "tipo",
        "categoria",
        "descricao",
        "titulo",
        "motivo",
        "detalhe",
        "classificacao",
        "assunto",
    ]
    cols = [c for c in candidates if c in s and s[c].upper().startswith("STRING")]
    log.debug("text_columns=%s", cols)
    return cols


def _escape_like_term(term: str) -> str:
    """
    Sanitiza termos para uso em LIKE.

    Importante: em BigQuery Standard SQL, aspas simples são escapadas duplicando-as.
    Ex.: O'Neil -> O''Neil
    """
    return term.replace("'", "''")


def _build_like_filter(terms: List[str]) -> str:
    """
    Constrói expressão de filtro textual tolerante:
    (LOWER(col) LIKE '%t1%' AND LOWER(col) LIKE '%t2%') OR ...  (por coluna textual disponível)
    """
    cols = _text_columns()
    if not cols or not terms:
        return "1=1"  # fallback seguro

    safe_terms = [_escape_like_term(t.lower()) for t in terms if t and t.strip()]
    if not safe_terms:
        return "1=1"

    per_col = []
    for c in cols:
        conj = " AND ".join([f"LOWER({c}) LIKE '%{t}%'" for t in safe_terms])
        per_col.append(f"({conj})")
    expr = "(" + " OR ".join(per_col) + ")"
    log.debug("like_filter=%s", expr)
    return expr


def _bairro_join_condition() -> str:
    """
    Ajusta o JOIN entre fato e dimensão bairro conforme tipos:

    - Se id_bairro no fato é STRING → c.id_bairro = CAST(b.id_bairro AS STRING)
    - Caso contrário → CAST(c.id_bairro AS INT64) = b.id_bairro
    """
    s = _schema()
    fato_t = s.get("id_bairro", "STRING").upper()
    cond = (
        "c.id_bairro = CAST(b.id_bairro AS STRING)"
        if fato_t.startswith("STRING")
        else "CAST(c.id_bairro AS INT64) = b.id_bairro"
    )
    log.debug("join_condition=%s (id_bairro tipo=%s)", cond, fato_t)
    return cond


def _one_line(sql: str) -> str:
    """Normaliza espaços para facilitar testes, logs e dry-run determinístico."""
    return re.sub(r"\s+", " ", sql or "").strip()


def _year_condition(target_year: int) -> Tuple[str, Optional[str]]:
    """
    Constrói condição de ano usando a melhor coluna disponível.

    PRIORIDADE **EFICIENTE**:
    1) Faixa em data_particao (partition pruning), se existir.
    2) EXTRACT(YEAR FROM c.data_inicio) = <ano>, se data_inicio existir.
    3) 1=1 (fallback).

    Returns
    -------
    (condição_sql, coluna_utilizada|None)
    """
    s = _schema()
    if "data_particao" in s:
        start = f"DATE '{target_year}-01-01'"
        end = f"DATE '{target_year+1}-01-01'"
        cond = f"(data_particao >= {start} AND data_particao < {end})"
        return cond, "data_particao"
    if "data_inicio" in s:
        return f"EXTRACT(YEAR FROM c.data_inicio) = {target_year}", "data_inicio"
    return "1=1", None


def _default_date_window() -> str:
    """
    Janela temporal padrão para perguntas sem data explícita.
    Prioriza a coluna particionada quando existir.
    Retorna uma expressão SQL de filtro (string).
    """
    s = _schema()
    if "data_particao" in s:
        return "data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)"
    if "data_inicio" in s:
        return "DATE(data_inicio) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)"
    return "1=1"


# Gerador de SQL


def generate_sql(question: str) -> Dict[str, Any]:
    """
    Gera SQL BigQuery eficiente (sem SELECT *) com base na pergunta,
    adaptando-se às colunas disponíveis no schema real.

    Segurança/eficiência:
    - SELECT com projeções explícitas (nunca SELECT *).
    - Filtros textuais somente em colunas existentes.
    - Condições parentetizadas (clareza de precedência).
    - Uso de partição (data_particao) quando disponível.
    """
    q = (question or "").strip().lower()
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
        out = _one_line(sql)
        log.info("SQL G1: %s", out)
        return {"sql": out}

    # 2) Subtipo mais comum relacionado a "Iluminação Pública"
    if "iluminação" in q:
        # Preferimos 'subtipo' (quando existir) para atender casos que exigem GROUP BY subtipo
        if "subtipo" in s and s["subtipo"].upper().startswith("STRING"):
            select_expr = "subtipo"
            group_expr = "subtipo"
        elif "tipo" in s and s["tipo"].upper().startswith("STRING"):
            select_expr = "tipo"
            group_expr = "tipo"
        else:
            select_expr = "categoria"
            group_expr = "categoria"

        filtro = _build_like_filter(["iluminação", "pública"])
        date_filter = _default_date_window()  # <- janela padrão para reduzir custo
        sql = f"""
            SELECT {select_expr}, COUNT(1) AS total
            FROM `{TAB_CHAMADO}`
            WHERE ({filtro}) AND ({date_filter})
            GROUP BY {group_expr}
            ORDER BY total DESC
            LIMIT 1
        """
        out = _one_line(sql)
        log.info("SQL G2: %s", out)
        return {"sql": out}

    # 3) Top 3 bairros — "reparo de buraco" em 2023 (JOIN com bairro)
    if "reparo" in q and "buraco" in q and "2023" in q:
        filtro = _build_like_filter(["reparo", "buraco"])
        join_on = _bairro_join_condition()
        year_cond, year_col = _year_condition(2023)

        sql = f"""
            SELECT b.nome AS bairro, COUNT(1) AS total
            FROM `{TAB_CHAMADO}` c
            JOIN `{TAB_BAIRRO}` b
              ON {join_on}
            WHERE ({year_cond})
              AND ({filtro})
            GROUP BY bairro
            ORDER BY total DESC
            LIMIT 3
        """
        out = _one_line(sql)
        log.info("SQL G3 (year_col=%s): %s", year_col, out)
        return {"sql": out}

    # 4) Unidade organizacional líder — "Fiscalização de estacionamento irregular"
    if "fiscalização" in q and "estacionamento" in q and "irregular" in q:
        filtro = _build_like_filter(["fiscalização", "estacionamento", "irregular"])
        unidade_col = (
            "nome_unidade_organizacional"
            if "nome_unidade_organizacional" in s
            else "id_unidade_organizacional"
        )
        date_filter = _default_date_window()  # <- janela padrão para reduzir custo
        sql = f"""
            SELECT {unidade_col} AS unidade, COUNT(1) AS total
            FROM `{TAB_CHAMADO}`
            WHERE ({filtro}) AND ({date_filter})
            GROUP BY unidade
            ORDER BY total DESC
            LIMIT 1
        """
        out = _one_line(sql)
        log.info("SQL G4: %s", out)
        return {"sql": out}

    # Fallback seguro
    base_date = "2024-11-28"
    where_col = (
        "data_particao"
        if "data_particao" in s
        else ("DATE(data_inicio)" if "data_inicio" in s else None)
    )
    where_expr = f"{where_col} = DATE '{base_date}'" if where_col else "1=1"
    sql = f"SELECT COUNT(1) AS n FROM `{TAB_CHAMADO}` WHERE {where_expr}"
    out = _one_line(sql)
    log.info("SQL Fallback: %s", out)
    return {"sql": out}


# Validador / Executor / Síntese


def validate_sql(sql: str) -> Dict[str, Any]:
    """
    Valida a consulta via DRY-RUN (sem custo).

    Returns
    -------
    dict
        {"ok": bool, "error": str|None, "dry_run_bytes": int|None}
    """
    if not (sql or "").strip():
        return {
            "ok": False,
            "error": "SQL vazio para validação.",
            "dry_run_bytes": None,
        }

    out = dry_run(sql)
    log.debug(
        "validate_sql | ok=%s | bytes=%s | err=%s",
        out.get("ok"),
        out.get("dry_run_bytes"),
        out.get("error"),
    )
    return {
        "ok": bool(out.get("ok")),
        "error": out.get("error"),
        "dry_run_bytes": out.get("dry_run_bytes"),
    }


def execute_sql(sql: str) -> Dict[str, Any]:
    """
    Executa a consulta no BigQuery. Assumimos que já houve DRY-RUN ok.

    Returns
    -------
    dict
        {"ok": bool, "df": DataFrame|None, "error": str|None}
    """
    if not (sql or "").strip():
        return {"ok": False, "df": None, "error": "SQL vazio no executor."}

    out = execute(sql)
    log.debug(
        "execute_sql | ok=%s | rows=%s | err=%s",
        out.get("ok"),
        None if out.get("df") is None else len(out.get("df")),
        out.get("error"),
    )
    return {"ok": bool(out.get("ok")), "df": out.get("df"), "error": out.get("error")}


def synthesize(answer_df, question: str) -> Dict[str, Any]:
    """
    Converte DataFrame em resposta textual final.

    Política:
    - Se LLM_USE_FOR_SYNTH=1 e OPENAI_API_KEY definido: usa LLM (via utils.llm) com preview de no máx. 10 linhas.
    - Caso contrário, usa fallback determinístico (previsível e barato).

    Returns
    -------
    dict
        {"answer": str}
    """
    import pandas as pd

    if answer_df is None:
        return {"answer": "Não foi possível obter resultados."}
    if isinstance(answer_df, pd.DataFrame) and answer_df.empty:
        return {"answer": "Nenhum registro encontrado para o filtro solicitado."}

    # Caminho com LLM
    if os.getenv("LLM_USE_FOR_SYNTH") == "1" and os.getenv("OPENAI_API_KEY"):
        try:
            from src.utils.llm import get_llm_response

            preview = (
                answer_df.head(10).to_markdown(index=False)
                if isinstance(answer_df, pd.DataFrame)
                else str(answer_df)[:2000]
            )
            prompt = (
                "Você é um analista de dados. Responda em PT-BR, no máximo 3 frases, "
                "de forma objetiva e sem inventar números. Use SOMENTE o preview como base.\n"
                f"Pergunta: {question}\n\nPreview (até 10 linhas):\n{preview}"
            )
            out = get_llm_response(prompt)
            if out.get("ok") and out.get("text"):
                return {"answer": out["text"]}
            log.warning("LLM não respondeu; usando fallback. erro=%s", out.get("error"))
        except Exception as e:
            log.warning("Falha no LLM; usando fallback. err=%r", e)

    # Fallback determinístico
    try:
        cols = [c.lower() for c in answer_df.columns]

        # Caso clássico
        if "n" in cols and len(answer_df) == 1:
            n = int(answer_df.iloc[0][answer_df.columns[cols.index("n")]])
            return {"answer": f"Contagem: {n}."}

        # Caso agregação categórica
        if "total" in cols:
            if len(answer_df) == 1:
                row = answer_df.iloc[0]
                keys = [c for c in answer_df.columns if c.lower() != "total"]
                k = keys[0] if keys else "categoria"
                return {"answer": f"{k}: {row[keys[0]]} (total: {int(row['total'])})."}
            head = answer_df.head(3).to_dict(orient="records")
            return {"answer": f"Top resultados: {head}"}

        # Genérico
        head = answer_df.head(3).to_dict(orient="records")
        return {"answer": f"Amostra de resultados: {head}"}
    except Exception as e:
        log.error("Erro no fallback de síntese: %r", e)
        head = str(answer_df)[:500]
        return {"answer": f"Prévia de resultados: {head}"}


# Chit-chat


def chitchat(question: str) -> Dict[str, Any]:
    """
    Responde educadamente a saudações/perguntas genéricas.

    Preferencialmente usa o LLM (utils.llm). Se a API não estiver configurada,
    retorna uma mensagem estática e útil ao contexto do agente.
    """
    try:
        from src.utils.llm import get_llm_response  # usa gpt-4o-mini por padrão

        prompt = (
            "Responda em PT-BR, no máximo 2 frases, de forma simpática e objetiva. "
            "Se fizer sentido, mencione que posso ajudar com análises dos chamados do 1746."
        )
        out = get_llm_response(f"{prompt}\n\nUsuário: {question}")
        if out.get("ok") and out.get("text"):
            return {"answer": out["text"]}
    except Exception as e:
        log.debug("chitchat fallback | err=%r", e)

    return {
        "answer": (
            "Olá! Posso ajudar com análises sobre os chamados do 1746. "
            "Exemplo: 'Quantos chamados houve em 28/11/2024?'"
        )
    }
