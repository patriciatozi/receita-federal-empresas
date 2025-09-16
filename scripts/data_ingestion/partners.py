import sys
import os
import psycopg2

sys.path.insert(0, '/opt/airflow/scripts')

from utils import get_source_data, save_to_postgres

def get_partners():
    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

    partner_columns = [
        "cnpj",
        "tipo_socio",
        "nome_socio",
        "documento_socio",
        "codigo_qualificacao_socio"
    ]

    partner_dtypes = {
        "cnpj": "string",
        "tipo_socio": "string",
        "nome_socio": "string", 
        "documento_socio": "string",
        "codigo_qualificacao_socio": "string"
    }

    df_socios = get_source_data(
        endpoint,
        "Socios",
        partner_columns,
        partner_dtypes
    )

    print(f"✅ Total de empresas extraídas: {len(df_socios):,}")
    print(df_socios.head())

    return df_socios


def main():
    """Função principal para execução direta do script"""
    try:
        df = get_partners()

        columns_table = {
            "cnpj": "TEXT",
            "tipo_socio": "TEXT",
            "nome_socio": "TEXT",
            "documento_socio": "TEXT",
            "codigo_qualificacao_socio": "TEXT"
        }

        db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "table": "bronze_partners"
        }

        save_to_postgres(df, columns_table, db_config)

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()