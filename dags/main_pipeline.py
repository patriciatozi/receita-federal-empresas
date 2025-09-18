from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_python_script(script_path: str):
    """Executa um script Python externo, logando saída e erros."""
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script não encontrado: {script_path}")

    print(f"Executando script: {script_path}")
    result = subprocess.run(
        ['python', script_path],
        capture_output=True,
        text=True,
        cwd='/opt/airflow/scripts'
    )

    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Script falhou com código {result.returncode}")

    print("Script executado com sucesso!")

# Caminhos dos scripts organizados por camada e nome
script_tasks = {
    "bronze_companies": "/opt/airflow/scripts/data_ingestion/companies.py",
    "bronze_partners": "/opt/airflow/scripts/data_ingestion/partners.py",
    "bronze_layer_dq": "/opt/airflow/scripts/data_quality/bronze_validation.py",
    "silver_companies": "/opt/airflow/scripts/data_processing/companies.py",
    "silver_partners": "/opt/airflow/scripts/data_processing/partners.py",
    "silver_layer_dq": "/opt/airflow/scripts/data_quality/silver_validation.py",
    "gold_companies_detail": "/opt/airflow/scripts/data_refinement/companies_detail.py",
    "gold_layer_dq": "/opt/airflow/scripts/data_quality/gold_validation.py",
}

with DAG(
    'main_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Cria os operadores dinamicamente
    tasks = {
        name: PythonOperator(
            task_id=name,
            python_callable=run_python_script,
            op_args=[path],
        )
        for name, path in script_tasks.items()
    }

    # Define as dependências
    [tasks["bronze_companies"], tasks["bronze_partners"]] >> tasks["bronze_layer_dq"]
    tasks["bronze_layer_dq"] >> [tasks["silver_companies"], tasks["silver_partners"]]
    [tasks["silver_companies"], tasks["silver_partners"]] >> tasks["silver_layer_dq"]
    tasks["silver_layer_dq"] >> tasks["gold_companies_detail"] >> tasks["gold_layer_dq"]
