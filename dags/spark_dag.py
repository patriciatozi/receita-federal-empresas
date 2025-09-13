from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def simple_spark_task():
    from pyspark.sql import SparkSession
    
    # Inicializar sessão Spark
    spark = SparkSession.builder \
        .appName("AirflowSpark") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Criar dados de exemplo
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["Language", "Users"]
    
    # Criar DataFrame
    df = spark.createDataFrame(data, columns)
    
    # Mostrar resultado
    print("=== DataFrame ===")
    df.show()
    
    # Contar linhas
    count = df.count()
    print(f"Total de linhas: {count}")
    
    # Transformação simples
    df_filtered = df.filter(df.Users > 5000)
    print("=== Linguagens com mais de 5000 usuários ===")
    df_filtered.show()
    
    # Parar sessão Spark
    spark.stop()
    
    return count

with DAG(
    'spark_example_dag',
    default_args=default_args,
    description='Um exemplo de DAG que executa tarefas Spark com Airflow 3.0.6',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['spark', 'pyspark'],
) as dag:

    python_spark_task = PythonOperator(
        task_id='python_spark_task',
        python_callable=simple_spark_task,
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        application='/opt/airflow/scripts/spark_script.py',
        conn_id='spark_default',
        verbose=True,
        application_args=['--input-path', '/opt/airflow/data/input', '--output-path', '/opt/airflow/data/output']
    )

    python_spark_task >> spark_submit_task