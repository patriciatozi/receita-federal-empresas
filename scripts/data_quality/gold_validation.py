from pandera import Column, DataFrameSchema, Check
import pandera as pa
import pandas as pd
import warnings
import sys
import os

warnings.filterwarnings("ignore")
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table, log_quality_metric, calculate_basic_metrics


def validate_gold_companies_detail(table):

    """Valida dados gold de empresas"""

    df = read_table(table)

    schema = DataFrameSchema({
        "cnpj": Column(str, unique=True, nullable=False, checks=Check.str_length(14, 14)),
        "cnpj": Column(str, unique=True, nullable=False),
        "flag_socio_estrangeiro": Column(bool, nullable=False),
        "doc_alvo": Column(bool, nullable=False)
    })

    stage = "gold"
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

        validate_gold_companies_detail("gold_companies_detail")

        print("✅ Todas as validações passaram!")

    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()