"""
Streamlit UI — Agente GenAI 1746 (Prefeitura do Rio)

Objetivo
--------
Interface web profissional para o agente em LangGraph que consulta o BigQuery (datario):
- Entrada de pergunta em linguagem natural
- Presets das 6 perguntas do desafio
- Resposta sintetizada (LLM on/off), SQL gerado e validação (dry-run)
- Preview do DataFrame, download de CSV e gráfico automático quando aplicável
- Métricas de observabilidade: intent, bytes estimados, latência, versão do grafo

Boas práticas aplicadas
-----------------------
- Configuração para GCP Cloud Shell (porta 8501, headless)
- Cache de resultados por 10 minutos (evita refazer consultas idênticas)
- Toggle "LLM on/off" que sobrepõe LLM_USE_FOR_SYNTH somente nesta execução
- Tolerância a falhas (erros aparecem em mensagens claras; nunca quebra a UI)
- Documentação e comentários para manutenção futura
- Sem exposição de segredos (nada de mostrar chaves/variáveis sensíveis)
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# API do agente (já existente no projeto):
    # - run_debug() retorna o estado completo do grafo 
    # - GRAPH_VERSION ajuda no rastreio de mudanças
from src.agent.graph import run_debug, GRAPH_VERSION



# Configuração da página

st.set_page_config(
    page_title="Agente GenAI 1746 — Prefeitura do Rio",
    page_icon="🧭",
    layout="wide",
)


# Presets das perguntas do desafio
PRESETS = [
    "Quantos chamados foram abertos no dia 28/11/2024?",
    "Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?",
    "Quais os 3 bairros que mais tiveram chamados abertos sobre reparo de buraco em 2023?",
    "Qual o nome da unidade organizacional que mais atendeu chamados de Fiscalização de estacionamento irregular?",
    "Olá, tudo bem?",
    "Me dê sugestões de brincadeiras para fazer com meu cachorro!",
]


# Utilitários
def _fmt_int(x: Optional[int | float]) -> str:
    """Formata inteiros/float para PT-BR (1.234.567) ou '—' se inválido."""
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        try:
            return f"{float(x):,.0f}".replace(",", ".")
        except Exception:
            return "—"


def _ptbr_number(n: Any) -> str:
    """Formatação PT-BR para rótulos do gráfico."""
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        try:
            return f"{float(n):,.0f}".replace(",", ".")
        except Exception:
            return str(n)


@st.cache_data(show_spinner=False, ttl=600)
def ask_agent_cached(question: str, llm_on: bool) -> Dict[str, Any]:
    """
    Invoca o agente e cacheia o resultado por 10 minutos (chave = pergunta + modo LLM).

    - Sobrepõe temporariamente LLM_USE_FOR_SYNTH com base no toggle da UI.
    - Restaura a variável ao final, evitando efeitos colaterais no processo.

    Returns
    -------
    state: Dict com as chaves usuais do grafo (intent, sql, df, answer, meta...)
    """
    prev = os.getenv("LLM_USE_FOR_SYNTH")
    try:
        os.environ["LLM_USE_FOR_SYNTH"] = "1" if llm_on else "0"
        state = run_debug(question or "")
        return state or {}
    finally:
        if prev is None:
            os.environ.pop("LLM_USE_FOR_SYNTH", None)
        else:
            os.environ["LLM_USE_FOR_SYNTH"] = prev


def _bar_with_value_labels(df: pd.DataFrame, cat_col: str, value_col: str) -> go.Figure:
    """
    Cria um gráfico de barras com:
    - Ordenação desc por valor
    - Rótulos de valor PT-BR acima de cada barra (textposition='outside')
    - Padding de eixo Y para evitar corte dos rótulos
    """
    tmp = df[[cat_col, value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce").fillna(0)
    tmp = tmp.sort_values(value_col, ascending=False)

    # Coluna de texto formatado
    tmp["__label"] = tmp[value_col].apply(_ptbr_number)

    fig = px.bar(tmp, x=cat_col, y=value_col, text="__label", title=None)
    fig.update_traces(textposition="outside", cliponaxis=False)

    vmax = float(tmp[value_col].max() or 0)
    pad = max(vmax * 0.12, 1.0)  
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=cat_col.capitalize(),
        yaxis_title="Total",
        yaxis=dict(range=[0, vmax + pad]),
        uniformtext_minsize=10,
        uniformtext_mode="hide",
    )
    return fig


def _viz_suggestion(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Decide automaticamente como visualizar o resultado:

    - Se existir coluna 'total' e exatamente 1 coluna categórica:
        * len(df) == 1  → metric card (label=cat, value=total)
        * len(df)  > 1  → bar chart COM RÓTULOS
    - Se existir 'n' (contagem simples) com 1 linha:
        * metric card (label="Contagem", value=n)
    - Caso contrário: None
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    cols_lower = [c.lower() for c in df.columns]

    # 1) Ranking categórico
    if "total" in cols_lower:
        cat_cols = [c for c in df.columns if c.lower() != "total"]
        if len(cat_cols) == 1:
            key = cat_cols[0]
            tmp = df[[key, "total"]].copy()
            tmp["total"] = pd.to_numeric(tmp["total"], errors="coerce").fillna(0)

            if len(tmp) == 1:
                label = str(tmp.iloc[0][key])
                val = tmp.iloc[0]["total"]
                return {
                    "type": "metric",
                    "title": f"{key.capitalize()} mais comum",
                    "value": _ptbr_number(val),
                    "subtitle": label,
                }
            else:
                fig = _bar_with_value_labels(tmp, cat_col=key, value_col="total")
                return {"type": "bar", "fig": fig}

    # 2) Contagem simples 'n'
    if "n" in cols_lower and len(df) == 1:
        try:
            n_val = float(df.iloc[0][df.columns[cols_lower.index("n")]])
        except Exception:
            n_val = None
        if n_val is not None:
            return {
                "type": "metric",
                "title": "Contagem",
                "value": _ptbr_number(n_val),
                "subtitle": "",
            }

    return None



# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Configurações")
    # Toggle do LLM
    llm_default = os.getenv("LLM_USE_FOR_SYNTH") == "1"
    llm_on = st.toggle(
        "Usar LLM na síntese",
        value=llm_default,
        help=(
            "Quando ligado, a resposta final usa LLM (se houver OPENAI_API_KEY). "
            "Quando desligado, usa fallback determinístico do agente."
        ),
    )
    max_rows = st.slider("Linhas do preview", min_value=3, max_value=50, value=8, step=1)
    st.divider()
    st.markdown("### Ambiente")
    st.write(f"**Projeto GCP**: `{os.getenv('PROJECT_ID', 'n/a')}`")
    st.write(f"**Localização BQ**: `{os.getenv('BQ_LOCATION', 'US')}`")
    st.write(f"**Graph**: v{GRAPH_VERSION}")
    st.caption(
        "A UI respeita as mesmas variáveis de ambiente do backend. "
        "Não exibimos segredos (ex.: OPENAI_API_KEY)."
    )



# Cabeçalho e quick actions
st.markdown("## Agente GenAI — Chamados 1746")
st.caption("Faça perguntas em linguagem natural. Use os atalhos das 6 questões do desafio para reproduzir o acceptance.")

cols = st.columns(3)
for i, q in enumerate(PRESETS):
    if cols[i % 3].button(q, use_container_width=True):
        st.session_state["current_q"] = q

question = st.text_input(
    "Pergunta",
    value=st.session_state.get("current_q", PRESETS[0]),
    placeholder="Ex.: Quantos chamados foram abertos no dia 28/11/2024?",
    label_visibility="collapsed",
)
run_clicked = st.button("Executar", type="primary")



# Execução principal
if run_clicked:
    q = (question or "").strip()
    if not q:
        st.warning("Digite uma pergunta para continuar.")
        st.stop()

    with st.spinner("Consultando o agente..."):
        try:
            state = ask_agent_cached(q, llm_on=llm_on)
        except Exception as e:
            st.error(f"Falha inesperada ao executar o agente: {e!r}")
            st.stop()

    # Normalização de estado
    meta = state.get("meta", {}) or {}
    intent = (state.get("intent") or "").lower()
    is_chitchat = intent == "chitchat"

    latency_ms = meta.get("latency_ms")
    dry_bytes = None if is_chitchat else meta.get("dry_run_bytes")
    validation_ok = None if is_chitchat else state.get("validation_ok")
    sql = None if is_chitchat else (state.get("sql") or meta.get("sql_preview"))
    df = None if is_chitchat else state.get("df")
    answer = state.get("answer") or "—"

    # KPIs (Intent / Dry-run / Latência / Validação)
    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1])
    c1.markdown(f"### {intent.upper() if intent else '—'}")
    c2.metric("Dry-run bytes", "—" if dry_bytes is None else _fmt_int(dry_bytes))
    c3.metric("Latência (ms)", "—" if latency_ms is None else _fmt_int(latency_ms))
    if validation_ok is True:
        c4.metric("Validação", "OK")
    elif validation_ok is False:
        c4.metric("Validação", "Falha")
    else:
        c4.metric("Validação", "N/A")

    # Resposta final
    if validation_ok is False:
        st.error(answer)
    else:
        st.success(answer)

    # Blocos apenas para perguntas de DADOS
    if not is_chitchat:
        # Prévia dos dados
        if isinstance(df, pd.DataFrame):
            with st.expander("Prévia dos dados", expanded=False):
                try:
                    st.dataframe(df.head(max_rows), use_container_width=True)
                except Exception:
                    st.write("Prévia indisponível para este resultado.")
                try:
                    st.download_button(
                        "Baixar CSV",
                        df.to_csv(index=False).encode("utf-8"),
                        file_name="resultado.csv",
                        mime="text/csv",
                    )
                except Exception:
                    st.caption("Não foi possível preparar o download do CSV.")

        # Visualização automática
        if isinstance(df, pd.DataFrame):
            viz = _viz_suggestion(df)
            if viz:
                st.markdown("#### Visualização")
                if viz["type"] == "bar":
                    st.plotly_chart(viz["fig"], use_container_width=True)
                elif viz["type"] == "metric":
                    cL, cR = st.columns([1, 2])
                    cL.metric(viz["title"], viz["value"])
                    if viz.get("subtitle"):
                        cR.markdown(f"**{viz['subtitle']}**")

        # SQL gerado / validação
        with st.expander("SQL gerado e validação", expanded=False):
            if sql:
                st.code(sql, language="sql")
            else:
                st.info("Nenhum SQL gerado (falha de geração).")
            if state.get("validation_error"):
                st.error(f"Erro de validação/execução: {state['validation_error']}")
            elif validation_ok is True:
                st.success("Dry-run OK")

        # Metadados
        with st.expander("Metadados", expanded=False):
            trimmed = dict(meta)
            if trimmed.get("sql_preview"):
                trimmed["sql_preview"] = str(trimmed["sql_preview"])[:800]
            st.json(trimmed)

# Rodapé
st.markdown("---")
st.caption(
    "© Prefeitura do Rio — Agente GenAI 1746 • "
    "LangGraph + BigQuery • Interface em Streamlit • "
    "Esta página não expõe segredos e respeita limites de custo/tempo do backend."
)