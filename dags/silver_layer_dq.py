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

def get_silver_layer_dq():
    """Executa o script Python usando subprocess com caminho absoluto"""
    # Caminho absoluto direto
    script_path = '/opt/airflow/scripts/data_quality/silver_validation.py'
    
    # Verifica se o arquivo existe
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script não encontrado: {script_path}")
    
    print(f"Executando script: {script_path}")
    
    # Executa o script
    result = subprocess.run(
        ['python', script_path], 
        capture_output=True, 
        text=True,
        cwd='/opt/airflow/scripts'  # Executa no diretório do script
    )
    
    # Log da saída
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Script falhou com código {result.returncode}")
    
    print("Script executado com sucesso!")

with DAG(
    'silver_layer_dq',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    silver_layer_dq = PythonOperator(
        task_id='silver_companies',
        python_callable=get_silver_layer_dq,
    )

    silver_layer_dq