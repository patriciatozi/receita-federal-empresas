import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import get_source_data, save_to_postgres, save_df_to_parquet

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
            "codigo_qualificacao_socio": "TEXT",
            "last_update": "TEXT"
        }

        save_df_to_parquet("./data/bronze/socios", df, ["last_update"])

        save_to_postgres(df, "bronze_partners", columns_table)

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()