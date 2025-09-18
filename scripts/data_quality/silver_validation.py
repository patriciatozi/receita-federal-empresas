from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, log_quality_metric, calculate_basic_metrics


def validate_silver_companies(table):

    """Valida dados silver de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, unique=True, nullable=False),
        "cod_porte": Column(str, checks=Check.isin(["00", "01", "03", "05"])),
        "natureza_juridica": Column(int, checks=Check.in_range(1011, 9999), nullable=False)
    })

    stage = "silver"
    calculate_basic_metrics(df, table, stage)

    try:

        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

        log_quality_metric(table, stage, "validation_success", 100, "ok")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")

        error_count = len(err.failure_cases)
        error_pct = (error_count / len(df)) * 100 if len(df) > 0 else 0
        
        status = 'error' if error_pct > 10 else 'warning'
        log_quality_metric(table, stage, 'validation_errors', error_count, status)
        log_quality_metric(table, stage, 'error_percentage', error_pct, status)

        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))  # mostra exemplo das linhas válidas

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

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

    stage = "silver"
    calculate_basic_metrics(df, table, stage)

    try:

        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

        log_quality_metric(table, stage, "validation_success", 100, "ok")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")

        error_count = len(err.failure_cases)
        error_pct = (error_count / len(df)) * 100 if len(df) > 0 else 0
        
        status = 'error' if error_pct > 10 else 'warning'
        log_quality_metric(table, stage, 'validation_errors', error_count, status)
        log_quality_metric(table, stage, 'error_percentage', error_pct, status)

        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))  # mostra exemplo das linhas válidas

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

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