from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, log_quality_metric, calculate_basic_metrics


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

    stage = "bronze"
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

    stage = "bronze"
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
        print(passed_checks.head(10))

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")

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