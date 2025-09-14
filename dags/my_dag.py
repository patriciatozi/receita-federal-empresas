from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="my_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    @task
    def verify_setup():
        import socket, subprocess, os

        print(f"current dir: {os.getcwd()}")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(('spark-master', 7077))
                print("✅ Conectado ao Spark Master")
        except Exception as e:
            print(f"❌ Erro conectando ao Spark Master: {e}")

        result = subprocess.run(['curl', '-s', 'http://spark-master:8081'],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Web UI do Spark acessível")
        else:
            print("❌ Web UI não acessível")

        return "Cluster verification completed"

    spark_job = SparkSubmitOperator(
        task_id="spark_read_job",
        application="/usr/local/airflow/include/scripts/read_with_spark.py",
        conn_id="my_spark_conn",
        verbose=True
    )

    @task
    def check_execution():
        import os
        data_dir = "/usr/local/airflow/include/data"

        result_files = []
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith(".csv") or file.endswith(".parquet") or "output" in file:
                    result_files.append(file)

        print(f"📊 Found {len(result_files)} result files: {result_files}")
        return f"✅ Job executed successfully! Results: {len(result_files)} files" if result_files else "⚠️ No results yet"

    verify_setup() >> spark_job >> check_execution()