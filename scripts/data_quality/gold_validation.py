import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from utils import read_table
sys.path.insert(0, '/opt/airflow/scripts')

from utils import read_table
import os

def validate_gold_data():
    """Valida dados gold consolidados"""

    context = ge.get_context()
    db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "table": "gold_companies_detail"
        }
    
    df = read_table(db_config,db_config["table"])
    
    expectation_suite = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "cnpj"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "qide_socios", "min_value": 1}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "flag_socio_estrangeiro", "value_set": [True, False]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "doc_alvo", "value_set": [True, False]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "cnpj"}
        )
    ]
    
    results = context.run_validation_using_pandas(
        dataframe=df,
        expectation_suite=expectation_suite,
        data_asset_name="gold_companies_detail"
    )
    
    if not results.success:
        raise Exception(f"Data Quality falhou para gold_companies_detail: {results}")
    
    return True