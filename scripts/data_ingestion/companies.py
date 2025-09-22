import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import get_source_data, save_to_postgres, save_df_to_parquet

def get_companies():

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
            "natureza_juridica": "INTEGER",
            "qualificacao_responsavel": "INTEGER",
            "capital_social": "TEXT",
            "cod_porte": "TEXT",
            "last_update": "TEXT"
        }

        save_df_to_parquet("./data/bronze/empresas", df, ["last_update"])

        save_to_postgres(df, "bronze_companies", columns_table, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise

if __name__ == "__main__":
    main()