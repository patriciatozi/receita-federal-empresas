import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, check_for_duplicates, save_to_postgres

def get_processed_companies(df):

    df.drop_duplicates(inplace=True)

    df["capital_social"] = (
        df["capital_social"]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False) 
        .astype(float)
    )

    return df


def main():
    """Função principal para execução direta do script"""
    try:

        df = read_table("bronze_companies")

        df_processed = get_processed_companies(df)

        not_duplicates = check_for_duplicates(df_processed)

        if not_duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "razao_social": "TEXT",
                "natureza_juridica": "BIGINT",
                "qualificacao_responsavel": "BIGINT",
                "capital_social": "FLOAT",
                "cod_porte": "TEXT"
            }

            save_to_postgres(df_processed, "silver_companies", columns_table, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()