from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table


def validate_gold_companies_detail(table):

    """Valida dados gold de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        # "cnpj": Column(str, unique=True, nullable=False, checks=Check.str_length(14, 14)),
        "cnpj": Column(str, unique=True, nullable=False),
        "flag_socio_estrangeiro": Column(bool, nullable=False),
        "doc_alvo": Column(bool, nullable=False)
    })

    try:
        # Validar e acumular erros
        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")

        # Mostrar checks que passaram
        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))  # mostra exemplo das linhas válidas

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

    return True

def main():
    try:

        validate_gold_companies_detail("gold_companies_detail")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()