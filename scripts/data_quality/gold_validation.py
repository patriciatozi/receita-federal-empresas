from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, validate_data_quality_table


def validate_gold_companies_detail(table):

    """Valida dados gold de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, unique=True, nullable=False, checks=Check.str_length(14, 14)),
        "qtde_socios": Column(int, nullable=False),
        "flag_socio_estrangeiro": Column(bool, nullable=False),
        "doc_alvo": Column(bool, nullable=False)
    })

    validate_data_quality_table(df, table, "gold", schema)

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