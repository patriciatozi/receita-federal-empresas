from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, validate_data_quality_table


def validate_silver_companies(table):

    """Valida dados silver de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, unique=True, nullable=False),
        "cod_porte": Column(str, checks=Check.isin(["00", "01", "03", "05"])),
        "natureza_juridica": Column(int, checks=Check.in_range(1011, 9999), nullable=False)
    })

    validate_data_quality_table(df, table, "silver", schema)

    return True


def validate_silver_partners(table):

    """Valida dados silver de sócios"""

    df = read_table(table)

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

    validate_data_quality_table(df, table, "silver", schema)

    return True


def main():
    try:

        validate_silver_companies("silver_companies")
        validate_silver_partners("silver_partners")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()