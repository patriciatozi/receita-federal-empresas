from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG
import os

def debug_directories():

    print("=== DEBUGANDO ESTRUTURA DE DIRETÓRIOS ===")
    print("Diretório atual:", os.getcwd())
    print("Caminho do arquivo DAG:", __file__)
    print("Diretório da DAG:", os.path.dirname(__file__))
    
    base_dir = '/opt/airflow'

    if os.path.exists(base_dir):
        
        print(f"\nConteúdo de {base_dir}:")

        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path):
                print(f"📁 {item}/")
            else:
                print(f"📄 {item}")
    
    scripts_path = '/opt/airflow/scripts'
    
    if os.path.exists(scripts_path):
        print(f"\nConteúdo de {scripts_path}:")
        for item in os.listdir(scripts_path):
            print(f"  - {item}")
    else:
        print(f"\n❌ Diretório {scripts_path} não existe!")

with DAG(
    'debug_directories',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    debug_task = PythonOperator(
        task_id='debug_directories',
        python_callable=debug_directories,
    )

    debug_task