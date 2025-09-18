"""
Smoke test para BigQuery no projeto 'genai-rio'.

Objetivo:
- Validar autenticação (ADC) no Cloud Shell.
- Confirmar acesso ao dataset público 'datario'.
- Executar consulta simples em uma partição específica.

Saída esperada:
- Impressão de um DataFrame com a contagem (coluna 'n') para a data 2024-11-28.
"""

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

PROJECT_ID = "genai-rio"

SQL = """
SELECT COUNT(1) AS n
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_particao = DATE '2024-11-28'
"""

def main() -> None:
    try:
        client = bigquery.Client(project=PROJECT_ID)
        job = client.query(SQL)
        df = job.result().to_dataframe()
        print("\nConsulta executada com sucesso. Resultado:")
        print(df)
        assert "n" in df.columns, "Coluna 'n' não encontrada no resultado."
        print("\nSmoke test OK")
    except GoogleAPIError as e:
        print("\nFalha na API do Google BigQuery.")
        print(repr(e))
        raise
    except Exception as e:
        print("\nErro inesperado no smoke test.")
        print(repr(e))
        raise

if __name__ == "__main__":
    main()