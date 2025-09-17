import os
import sys
import pandas as pd
import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

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
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        raise Exception(f"❌ Data Quality falhou para {table}:\n{err.failure_cases}")

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
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        raise Exception(f"❌ Data Quality falhou para {table}:\n{err.failure_cases}")

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