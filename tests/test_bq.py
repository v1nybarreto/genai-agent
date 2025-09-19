"""
Testes mínimos para utilitários de BigQuery.

Critérios:
- Dry-run de uma consulta simples deve passar.
- Execução da mesma consulta deve retornar DataFrame com coluna 'n'.
- Guarda anti-DML deve bloquear comandos perigosos.
"""

from src.utils.bq import dry_run, execute

SQL_OK = """
SELECT COUNT(1) AS n
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_particao = DATE '2024-11-28'
"""

SQL_BLOCKED = "DELETE FROM `datario.adm_central_atendimento_1746.chamado` WHERE TRUE"

def test_dry_run_ok():
    out = dry_run(SQL_OK)
    assert out["ok"] is True
    assert isinstance(out.get("dry_run_bytes"), int)

def test_execute_ok():
    out = execute(SQL_OK)
    assert out["ok"] is True
    df = out["df"]
    assert "n" in df.columns

def test_guard_blocks_dml():
    out = dry_run(SQL_BLOCKED)
    assert out["ok"] is False
    assert "Apenas SELECT" in out["error"]