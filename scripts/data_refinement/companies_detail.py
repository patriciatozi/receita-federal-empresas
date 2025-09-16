import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, check_for_duplicates, save_to_postgres

def get_processed_companies_detail(db_config):

    df_companies = read_table(db_config, "silver_companies")

    df_partners = read_table(db_config, "silver_partners")
    df_partners_processed = df_partners.groupby("cnpj", dropna=False).agg(
        qtd_socios=("documento_socio", "count"),           # total de sócios
        qtd_estrangeiros=("flag_socio_estrangeiro", "sum") # total de estrangeiros
    ).reset_index()

    df_result = (
        ((df_companies[["cnpj", "cod_porte"]]).drop_duplicates())
        .merge(
            df_partners_processed.drop_duplicates(),
            "inner",
            "cnpj"
        )
    )

    df_result["doc_alvo"] = df_result.apply(
        lambda row: True if row["cod_porte"] == "03" and row["qtd_socios"] > 1 else False,
        axis=1
    )

    df_result["flag_socio_estrangeiro"] = df_result["qtd_estrangeiros"] > 0

    df_result = df_result[["cnpj", "qtd_socios", "flag_socio_estrangeiro", "doc_alvo"]].drop_duplicates()

    return df_result

def main():
    """Função principal para execução direta do script"""
    try:
        db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "table": "gold_companies_detail"
        }

        df_processed = get_processed_companies_detail(db_config)

        duplicates = check_for_duplicates(df_processed)

        if duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "qtd_socios": "BIGINT",
                "flag_socio_estrangeiro": "BOOLEAN",
                "doc_alvo": "BOOLEAN"
            }

            save_to_postgres(df_processed, columns_table, db_config, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()