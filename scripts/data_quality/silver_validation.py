from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table


def validate_silver_companies(db_config, table):

    """Valida dados silver de empresas"""

    df = read_table(db_config, table)

    schema = DataFrameSchema({
        "cnpj": Column(str, unique=True, nullable=False),
        "cod_porte": Column(str, checks=Check.isin(["00", "01", "03", "05"])),
        "natureza_juridica": Column(int, checks=Check.in_range(1011, 9999), nullable=False)
    })

    try:
        # Validar e acumular erros
        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")
        # for _, row in failure_df.iterrows():
        #     print(
        #         f"  - Coluna: {row['column']}, "
        #         f"Valor inválido: {row['failure_case']}, "
        #         f"Check: {row['check']}, "
        #         f"Linha: {row['index']}"
        #     )

        # Mostrar checks que passaram
        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))  # mostra exemplo das linhas válidas

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

    return True


def validate_silver_partners(db_config, table):

    """Valida dados silver de sócios"""

    df = read_table(db_config, table)

    # Forçar conversão para Int64 nullable (suporta NaNs)
    df["codigo_qualificacao_socio"] = pd.to_numeric(
        df["codigo_qualificacao_socio"], errors="coerce"
    ).astype("Int64")

    df["tipo_socio"] = pd.to_numeric(
        df["tipo_socio"], errors="coerce"
    ).astype("Int64")

    schema = DataFrameSchema({
        "cnpj": Column(str, nullable=False),
        "tipo_socio": Column("Int64", checks=Check.isin([1, 2, 3]), nullable=True),
        "codigo_qualificacao_socio": Column("Int64", nullable=True),
    })

    try:
        # Validar e acumular erros
        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")
        # for _, row in failure_df.iterrows():
        #     print(
        #         f"  - Coluna: {row['column']}, "
        #         f"Valor inválido: {row['failure_case']}, "
        #         f"Check: {row['check']}, "
        #         f"Linha: {row['index']}"
        #     )

        # Mostrar checks que passaram
        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))  # mostra exemplo das linhas válidas

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

    return True


def main():
    try:
        db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
        }

        validate_silver_companies(db_config, "silver_companies")
        validate_silver_partners(db_config, "silver_partners")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()