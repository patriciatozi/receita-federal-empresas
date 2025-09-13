# my_dag.py
from airflow.decorators import dag, task
from datetime import datetime
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
    
    @task
    def verify_setup():
        """Verificar se o cluster Spark está funcionando"""
        import subprocess
        import socket
        
        print(f"current dir: {os.getcwd()}")

        # Testar conexão com Spark Master
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(('spark-master', 7077))
                print("✅ Conectado ao Spark Master na porta 7077")
        except Exception as e:
            print(f"❌ Erro conectando ao Spark Master: {e}")
        
        # Testar Web UI
        result = subprocess.run(['curl', '-s', 'http://spark-master:8081'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Web UI do Spark está acessível")
        else:
            print("❌ Web UI do Spark não está acessível")
        
        return "Cluster verification completed"
    
    @task
    def trigger_spark_job():
        """Criar arquivo de trigger para o watcher processar"""

        print(f"current dir: {os.getcwd()}")

        trigger_dir = "/usr/local/airflow/apps/triggers"
        os.makedirs(trigger_dir, exist_ok=True)
        
        trigger_file = os.path.join(trigger_dir, f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.trigger")
        
        with open(trigger_file, 'w') as f:
            f.write(f"EXECUTE_READ_PY {datetime.now()}")
        
        print(f"✅ Trigger file created: {trigger_file}")
        return f"Trigger created: {os.path.basename(trigger_file)}"
    
    @task
    def check_execution():
        """Verificar se o job foi executado com sucesso"""
        import time
        import glob

        print(f"current dir: {os.getcwd()}")
        
        # Dar tempo para execução (30 segundos)
        time.sleep(30)
        
        # Verificar se há arquivos de resultado
        result_files = []
        data_dir = "/usr/local/airflow/include/data"
        
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith('.csv') or file.endswith('.parquet') or 'output' in file:
                    result_files.append(file)
        
        # Verificar arquivos processados
        processed_dir = "/usr/local/airflow/apps/processed"
        processed_files = []
        if os.path.exists(processed_dir):
            processed_files = os.listdir(processed_dir)
        
        print(f"📊 Found {len(result_files)} result files: {result_files}")
        print(f"📊 Found {len(processed_files)} processed triggers: {processed_files}")
        
        if result_files or processed_files:
            return f"✅ Job executed successfully! Results: {len(result_files)} files"
        else:
            return "⚠️  No results found yet. Job may still be running."
    
    # Definir o fluxo da DAG
    verification = verify_setup()
    trigger_task = trigger_spark_job()
    check_task = check_execution()
    
    verification >> trigger_task >> check_task

my_dag()