from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, validate_data_quality_table


def validate_bronze_companies(table):

    """Valida dados bronze de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, nullable=False, checks=Check.str_matches(r'^\d+$')),
        "razao_social": Column(str, nullable=False),
        "natureza_juridica": Column(int, nullable=True),
        "qualificacao_responsavel": Column(int, nullable=True),
        "capital_social": Column(str, nullable=False),
        "cod_porte": Column(str, nullable=True)
    })

    validate_data_quality_table(df, table, "bronze", schema)

    return True


def validate_bronze_partners(table):

    """Valida dados bronze de sócios"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, nullable=False, checks=Check.str_matches(r'^\d+$')),
        "tipo_socio": Column(str, nullable=True),
        "nome_socio": Column(str, nullable=True),
        "documento_socio": Column(str, nullable=True),
        "codigo_qualificacao_socio": Column(str, nullable=True)
    })

    validate_data_quality_table(df, table, "bronze", schema)

    return True


def main():
    try:

        validate_bronze_companies("bronze_companies")
        validate_bronze_partners("bronze_partners")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()