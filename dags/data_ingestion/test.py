from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Função que será executada
def print_hello():
    print("Hello World")

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='DAG simples que imprime Hello World',
    schedule=None,  # substitui o antigo schedule_interval
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['exemplo'],
) as dag:

    task_hello = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )

    task_hello