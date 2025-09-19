"""
Testes para o roteador e o gerador de SQL.
"""

from src.agent.nodes import route_intent, generate_sql

def _norm(s: str) -> str:
    import re
    return re.sub(r"\s+", " ", s).strip().lower()

def test_route_chitchat():
    r = route_intent("Olá, tudo bem?")
    assert r["intent"] == "chitchat"

def test_route_data():
    r = route_intent("Quantos chamados foram abertos no dia 28/11/2024?")
    assert r["intent"] == "data"

def test_sql_no_star_and_has_table():
    sql = generate_sql("Quantos chamados foram abertos no dia 28/11/2024?")["sql"]
    n = _norm(sql)
    assert "select *" not in n
    assert "datario.adm_central_atendimento_1746.chamado" in n
    assert "data_particao" in n

def test_sql_iluminacao_subtipo():
    sql = generate_sql("Qual o subtipo de chamado mais comum relacionado a Iluminação Pública?")["sql"]
    n = _norm(sql)
    assert "group by subtipo" in n
    assert "order by total desc" in n
    assert "limit 1" in n

def test_sql_reparo_buraco_bairros():
    sql = generate_sql("Quais os 3 bairros que mais tiveram chamados abertos sobre reparo de buraco em 2023?")["sql"]
    n = _norm(sql)
    assert "join" in n and "dados_mestres.bairro" in n
    assert "extract(year from c.data_inicio) = 2023" in n
    assert "limit 3" in n

def test_sql_fiscalizacao_estacionamento_irregular():
    sql = generate_sql("Qual o nome da unidade organizacional que mais atendeu chamados de Fiscalização de estacionamento irregular?")["sql"]
    n = _norm(sql)
    assert "nome_unidade_organizacional" in n
    assert "order by total desc" in n
    assert "limit 1" in n