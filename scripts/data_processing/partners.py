import sys
import os

sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, check_for_duplicates, save_to_postgres

def get_processed_partners(df):

    df_processed = (
        df
        .sort_values(by=["cnpj", "documento_socio"], ascending=[False, False])  
        .drop_duplicates(subset=["cnpj", "documento_socio"], keep="first")
    )


    duplicates = check_for_duplicates(df_processed)

    if duplicates:

        df_processed = df_processed[
            ~(
                ((df_processed["nome_socio"].isna()) & (df_processed["documento_socio"].isna())) |
                ((df_processed["nome_socio"].isna()) & (df_processed["documento_socio"] == "***000000**"))
            )
        ].copy()

        df_processed["flag_socio_estrangeiro"] = (
            df_processed["documento_socio"]
            .fillna("")  # substitui NA por string vazia
            .eq("***999999**")  # compara com o valor desejado
            .astype(int)  # converte True/False para 1/0
        )

        df_processed["tipo_socio"] = df_processed["tipo_socio"].astype(int)

        return df_processed


def main():
    """Função principal para execução direta do script"""
    try:

        df = read_table("bronze_partners")

        df_processed = get_processed_partners(df)

        not_duplicates = check_for_duplicates(df_processed)

        if not_duplicates:

            columns_table = {
                "cnpj": "TEXT",
                "tipo_socio": "BIGINT",
                "nome_socio": "TEXT",
                "documento_socio": "TEXT",
                "codigo_qualificacao_socio": "TEXT",
                "flag_socio_estrangeiro": "BIGINT"
            }

            save_to_postgres(df_processed, "silver_partners", columns_table, ["cnpj", "documento_socio"])

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()