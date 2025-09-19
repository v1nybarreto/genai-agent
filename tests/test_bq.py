"""
Testes mínimos e de guarda para utilitários de BigQuery.

Critérios cobertos
------------------
- Dry-run de uma consulta simples deve passar e retornar estimativa de bytes.
- Execução da mesma consulta deve retornar DataFrame com coluna 'n'.
- Guarda anti-DML/DDL deve bloquear comandos perigosos.
- Bloqueio de múltiplas sentenças em uma mesma string de SQL.
- Bloqueio de 'SELECT *' (projeções devem ser explícitas).
- Aceitação de CTEs (WITH ... SELECT) como SELECT-only válido.
- Respeito ao teto de bytes faturáveis (maximum_bytes_billed).
"""

import importlib

import src.utils.bq as bq  # importa como módulo para permitir reload nos testes


# Consulta simples e barata (usa partição por data)
SQL_OK = """
SELECT COUNT(1) AS n
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_particao = DATE '2024-11-28'
"""

# DML explícito para validar o guard
SQL_BLOCKED = "DELETE FROM `datario.adm_central_atendimento_1746.chamado` WHERE TRUE"

# Múltiplas sentenças (também deve ser bloqueado pelo guard)
SQL_MULTI = "SELECT 1; SELECT 2"

# SELECT * deve ser bloqueado (projeções precisam ser explícitas)
SQL_SELECT_STAR = (
    "SELECT * FROM `datario.adm_central_atendimento_1746.chamado` WHERE FALSE"
)

# CTE simples (WITH ... SELECT) deve ser aceito como SELECT-only
SQL_WITH_OK = """
WITH t AS (SELECT 1 AS x)
SELECT COUNT(1) AS n FROM t
"""

# Execução imediata (administrativo) deve ser bloqueada
SQL_EXECUTE_IMMEDIATE = "EXECUTE IMMEDIATE 'SELECT 1'"


def test_dry_run_ok():
    """Dry-run deve validar e retornar bytes estimados."""
    out = bq.dry_run(SQL_OK)
    assert out["ok"] is True
    assert isinstance(out.get("dry_run_bytes"), int)
    assert out["dry_run_bytes"] >= 0


def test_execute_ok():
    """Execução deve retornar DataFrame com a coluna 'n' (contagem)."""
    out = bq.execute(SQL_OK)
    assert out["ok"] is True
    df = out["df"]
    assert df is not None
    assert "n" in df.columns


def test_guard_blocks_dml():
    """Comando DML/DDL deve ser rejeitado com mensagem padrão."""
    out = bq.dry_run(SQL_BLOCKED)
    assert out["ok"] is False
    # Mensagem padrão esperada pelo projeto
    assert "Apenas SELECT" in out["error"]


def test_guard_blocks_multi_statement():
    """Múltiplas sentenças na mesma string devem ser bloqueadas."""
    out = bq.dry_run(SQL_MULTI)
    assert out["ok"] is False
    assert "Apenas SELECT" in out["error"]


def test_guard_blocks_select_star():
    """'SELECT *' deve ser bloqueado (exigir projeções explícitas)."""
    out = bq.dry_run(SQL_SELECT_STAR)
    assert out["ok"] is False
    assert "SELECT *" in out["error"]


def test_cte_with_select_ok():
    """CTE (WITH ... SELECT) é permitido como SELECT-only."""
    out = bq.dry_run(SQL_WITH_OK)
    assert out["ok"] is True
    # Execução também deve funcionar
    run = bq.execute(SQL_WITH_OK)
    assert run["ok"] is True
    assert "n" in run["df"].columns


def test_guard_blocks_execute_immediate():
    """EXECUTE IMMEDIATE (administrativo) deve ser bloqueado."""
    out = bq.dry_run(SQL_EXECUTE_IMMEDIATE)
    assert out["ok"] is False
    assert "Apenas SELECT" in out["error"]


def test_execute_respects_max_bytes_billed(monkeypatch):
    """
    O executor deve respeitar o teto de bytes faturáveis.
    Força um teto artificialmente baixo e valida a recusa.
    """
    # Define um teto irreal (1 byte) e refaz o import para recarregar as constantes
    monkeypatch.setenv("BQ_MAX_BYTES_BILLED", "1")
    importlib.reload(bq)

    out = bq.execute(SQL_OK)
    assert out["ok"] is False
    assert "Custo estimado alto" in (out.get("error") or "")

    # Limpeza: restaura variável e módulo original para não afetar outros testes
    monkeypatch.delenv("BQ_MAX_BYTES_BILLED", raising=False)
    importlib.reload(bq)
