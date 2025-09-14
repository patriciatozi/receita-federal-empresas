from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "pyspark"]
)
def new_dag():

    @task
    def verify_spark():
        """Verifica se o cluster Spark está acessível"""
        import socket

        try:
            with socket.create_connection(("spark-master", 7077), timeout=5):
                print("✅ Conectado ao Spark Master na porta 7077")
        except Exception as e:
            raise Exception(f"❌ Erro conectando ao Spark Master: {e}")

    spark_task = SparkSubmitOperator(
        task_id="spark_read_job",
        application="/usr/local/airflow/include/scripts/read_with_spark.py",
        conn_id="my_spark_conn",  # conexão Spark configurada no Astro
        verbose=True,
        name="arrow-spark",
    )

    verify_spark() >> spark_task

new_dag_instance = new_dag()