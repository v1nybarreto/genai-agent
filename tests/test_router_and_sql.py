"""
Testes para o roteador e o gerador de SQL.

Cobertura
---------
- Roteamento de perguntas em linguagem natural → intent correto.
- Geração de SQL sem SELECT * e apontando para a tabela correta.
- Casos específicos: iluminação pública, reparo de buraco, fiscalização de estacionamento irregular.
- Janela temporal defensiva (últimos 365 dias) quando apropriado.
- Filtro textual com LOWER(...) LIKE '%...%'.
"""

import re
from src.agent.nodes import route_intent, generate_sql


def _norm(s: str) -> str:
    """Normaliza espaços e caixa para facilitar matching."""
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def test_route_chitchat():
    """Pergunta genérica deve ser roteada para chit-chat."""
    r = route_intent("Olá, tudo bem?")
    assert r["intent"] == "chitchat"


def test_route_data():
    """Pergunta sobre contagem de chamados deve ser roteada para dados."""
    r = route_intent("Quantos chamados foram abertos no dia 28/11/2024?")
    assert r["intent"] == "data"


def test_sql_no_star_and_has_table():
    """SQL gerado não deve ter SELECT * e deve referenciar tabela + partição."""
    sql = generate_sql("Quantos chamados foram abertos no dia 28/11/2024?")["sql"]
    n = _norm(sql)

    assert "select *" not in n
    assert "datario.adm_central_atendimento_1746.chamado" in n
    # Deve aplicar filtro por data (idealmente data_particao; se não existir, data_inicio)
    assert "data_particao" in n or "data_inicio" in n


def test_sql_iluminacao_subtipo():
    """Pergunta sobre iluminação pública deve gerar agrupamento por subtipo."""
    sql = generate_sql(
        "Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?"
    )["sql"]
    n = _norm(sql)

    assert "group by subtipo" in n
    assert "order by total desc" in n
    assert "limit 1" in n


def test_sql_iluminacao_uses_default_window():
    """Consulta de iluminação deve aplicar janela temporal padrão (365 dias)."""
    sql = generate_sql(
        "Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?"
    )["sql"]
    n = _norm(sql)
    # A janela padrão deve aparecer (via data_particao ou data_inicio)
    assert "date_sub(current_date(), interval 365 day)" in n


def test_sql_iluminacao_like_terms():
    """Consulta de iluminação deve conter filtros LIKE com termos exigidos."""
    sql = generate_sql(
        "Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?"
    )["sql"]
    n = _norm(sql)
    # Não dependemos da coluna específica (subtipo/tipo/categoria), apenas dos termos
    assert "like '%iluminação%'" in n
    assert "like '%pública%'" in n


def test_sql_reparo_buraco_bairros():
    """Pergunta sobre reparo de buraco deve gerar JOIN com bairros e filtro por ano."""
    sql = generate_sql(
        "Quais os 3 bairros que mais tiveram chamados abertos sobre reparo de buraco em 2023?"
    )["sql"]
    n = _norm(sql)

    assert "join" in n and "dados_mestres.bairro" in n
    # Condição de ano (via data_inicio ou data_particao)
    assert "2023" in n
    assert "limit 3" in n


def test_sql_fiscalizacao_estacionamento_irregular():
    """Pergunta sobre fiscalização irregular deve agrupar por unidade organizacional."""
    sql = generate_sql(
        "Qual o nome da unidade organizacional que mais atendeu chamados de Fiscalização de estacionamento irregular?"
    )["sql"]
    n = _norm(sql)

    assert "nome_unidade_organizacional" in n or "id_unidade_organizacional" in n
    assert "order by total desc" in n
    assert "limit 1" in n


def test_sql_fiscalizacao_uses_default_window():
    """Consulta de fiscalização irregular deve aplicar janela temporal padrão (365 dias)."""
    sql = generate_sql(
        "Qual o nome da unidade organizacional que mais atendeu chamados de Fiscalização de estacionamento irregular?"
    )["sql"]
    n = _norm(sql)
    assert "date_sub(current_date(), interval 365 day)" in n
