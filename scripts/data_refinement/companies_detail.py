import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, check_for_duplicates, save_to_postgres, get_last_update, save_df_to_parquet

def get_processed_companies_detail():

    df_companies = read_table("silver_companies")

    df_partners = read_table("silver_partners")
    df_partners_processed = df_partners.groupby("cnpj", dropna=False).agg(
        qtde_socios=("documento_socio", "count"),
        qtd_estrangeiros=("flag_socio_estrangeiro", "sum")
    ).reset_index()

    df_result = (
        ((df_companies[["cnpj", "cod_porte"]]).drop_duplicates())
        .merge(
            df_partners_processed.drop_duplicates(),
            "inner",
            "cnpj"
        )
    )

    df_result["cnpj"] = df_result["cnpj"].str.zfill(14)

    df_result["doc_alvo"] = df_result.apply(
        lambda row: True if row["cod_porte"] == "03" and row["qtde_socios"] > 1 else False,
        axis=1
    )

    df_result["flag_socio_estrangeiro"] = df_result["qtd_estrangeiros"] > 0

    df_result = df_result[["cnpj", "qtde_socios", "flag_socio_estrangeiro", "doc_alvo"]].drop_duplicates()

    df_result["last_update"] = get_last_update()

    return df_result

def main():
    """Função principal para execução direta do script"""
    try:

        df_processed = get_processed_companies_detail()

        duplicates = check_for_duplicates(df_processed)

        if duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "qtde_socios": "INTEGER",
                "flag_socio_estrangeiro": "BOOLEAN",
                "doc_alvo": "BOOLEAN",
                "last_update": "DATE"
            }

            save_df_to_parquet("./data/gold/empresas_detalhe", df_processed, ["last_update"])

            save_to_postgres(df_processed, "gold_companies_detail", columns_table, ["cnpj"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()