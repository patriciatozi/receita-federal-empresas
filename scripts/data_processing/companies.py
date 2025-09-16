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
        db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "table": "silver_companies"
        }

        df = read_table(db_config, "bronze_companies")

        df_processed = get_processed_companies(df)

        duplicates = check_for_duplicates(df_processed)

        if duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "razao_social": "TEXT",
                "natureza_juridica": "BIGINT",
                "qualificacao_responsavel": "BIGINT",
                "capital_social": "FLOAT",
                "cod_porte": "TEXT"
            }

            save_to_postgres(df_processed, columns_table, db_config, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()