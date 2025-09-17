from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table


def validate_bronze_companies(db_config, table):

    """Valida dados bronze de empresas"""

    df = read_table(db_config, table)

    schema = DataFrameSchema({
        "cnpj": Column(str, nullable=False, checks=Check.str_matches(r'^\d+$')),
        "razao_social": Column(str, nullable=False),
        "natureza_juridica": Column(int, nullable=True),
        "qualificacao_responsavel": Column(int, nullable=True),
        "capital_social": Column(str, nullable=False),
        "cod_porte": Column(str, nullable=True)
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


def validate_bronze_partners(db_config, table):

    """Valida dados bronze de sócios"""

    df = read_table(db_config, table)

    schema = DataFrameSchema({
        "cnpj": Column(str, nullable=False, checks=Check.str_matches(r'^\d+$')),
        "tipo_socio": Column(str, nullable=True),
        "nome_socio": Column(str, nullable=True),
        "documento_socio": Column(str, nullable=True),
        "codigo_qualificacao_socio": Column(str, nullable=True)
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

        validate_bronze_companies(db_config, "bronze_companies")
        validate_bronze_partners(db_config, "bronze_partners")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()