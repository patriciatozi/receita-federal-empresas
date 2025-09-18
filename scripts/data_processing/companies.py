import datetime
import sys

sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, check_for_duplicates, save_to_postgres, get_last_update, save_df_to_parquet

def get_processed_companies(df):

    df.drop_duplicates(inplace=True)

    df["capital_social"] = (
        df["capital_social"]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False) 
        .astype(float)
    )

    df["last_update"] = get_last_update()

    return df


def main():
    """Função principal para execução direta do script"""
    try:

        df = read_table("bronze_companies").drop("last_update", axis=1)

        df_processed = get_processed_companies(df)

        not_duplicates = check_for_duplicates(df_processed)

        if not_duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "razao_social": "TEXT",
                "natureza_juridica": "BIGINT",
                "qualificacao_responsavel": "BIGINT",
                "capital_social": "FLOAT",
                "cod_porte": "TEXT",
                "last_update": "DATE"
            }

            save_df_to_parquet("./data/silver/empresas", df_processed, ["last_update"])

            save_to_postgres(df_processed, "silver_companies", columns_table, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()