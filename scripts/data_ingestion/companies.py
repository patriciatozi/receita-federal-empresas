import sys
import os
import psycopg2

sys.path.insert(0, '/opt/airflow/scripts')

from utils import get_source_data, save_to_postgres

def get_companies():
    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

    company_columns = [
        "cnpj",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "cod_porte"
    ]

    company_dtypes = {
        "cnpj": "string",
        "razao_social": "string",
        "natureza_juridica": "Int64", 
        "qualificacao_responsavel": "Int64",
        "capital_social": "string",
        "cod_porte": "string"
    }

    df_empresas = get_source_data(
        endpoint,
        "Empresas",
        company_columns,
        company_dtypes
    )

    print(f"✅ Total de empresas extraídas: {len(df_empresas):,}")
    print(df_empresas.head())

    return df_empresas


def main():
    """Função principal para execução direta do script"""
    try:
        df = get_companies()

        columns_table = {
            "cnpj": "TEXT",
            "razao_social": "TEXT",
            "natureza_juridica": "BIGINT",
            "qualificacao_responsavel": "BIGINT",
            "capital_social": "TEXT",
            "cod_porte": "TEXT",
            "last_update": "TEXT"
        }

        db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "table": "bronze_companies"
        }

        df.to_parquet("./data/bronze/empresas", engine="pyarrow", index=False, partition_cols=["last_update"])

        save_to_postgres(df, columns_table, db_config, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise

if __name__ == "__main__":
    main()