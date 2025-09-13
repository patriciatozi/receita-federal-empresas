from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="empresas_receita_dag",
    default_args=default_args,
    description="DAG que executa job Spark das empresas da Receita",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["spark", "cnpj"],
) as dag:

    spark_task = BashOperator(
        task_id="processa_empresas",
        bash_command="spark-submit --master local[*] /opt/airflow/jobs/data_ingestion/companies.py",
    )

    spark_task