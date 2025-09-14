from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import os
import time

class DatabricksClient:
    def __init__(self):
        self.host = os.getenv('DATABRICKS_HOST')
        self.token = os.getenv('DATABRICKS_TOKEN')
        self.client = WorkspaceClient(host=self.host, token=self.token)
    
    def submit_python_job(self, python_file):
        """Submete job Python usando Serverless Starter Warehouse"""
        try:
            # Configuração do job para Serverless Warehouse
            job_settings = {
                "name": f"CNPI Pipeline - {os.path.basename(python_file)}",
                "tasks": [{
                    "task_key": "main_task",
                    "spark_python_task": {
                        "python_file": python_file
                    },
                    # Usar Serverless Starter Warehouse
                    "existing_cluster_id": "5e2035d2a5985127"  # ID do seu warehouse
                }],
                "max_concurrent_runs": 1
            }
            
            job = self.client.jobs.create(**job_settings)
            run = self.client.jobs.run_now(job_id=job.job_id)
            print(f"✅ Job submetido com ID: {run.run_id}")
            return run.run_id
            
        except Exception as e:
            print(f"❌ Erro ao submeter job: {e}")
            return None
    
    def wait_for_completion(self, run_id, timeout_minutes=30):
        """Aguarda a conclusão do job"""
        start_time = time.time()
        
        while time.time() - start_time < timeout_minutes * 60:
            try:
                run_info = self.client.jobs.get_run(run_id=run_id)
                state = run_info.state
                
                if state:
                    print(f"Job status: {state.life_cycle_state}")
                    
                    if state.life_cycle_state in ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']:
                        if state.result_state == 'SUCCESS':
                            return True
                        else:
                            print(f"Job result: {state.result_state}")
                            if state.state_message:
                                print(f"Message: {state.state_message}")
                            return False
                
                time.sleep(30)
            except Exception as e:
                print(f"Erro ao verificar status: {e}")
                time.sleep(30)
        
        return False

    def upload_file(self, local_path, workspace_path):
        """Faz upload de arquivo para Workspace"""
        print(f"📤 Uploading {local_path} to {workspace_path}")
        
        try:
            # Criar diretório se necessário
            parent_dir = os.path.dirname(workspace_path)
            if parent_dir and parent_dir != "/":
                try:
                    self.client.workspace.mkdirs(parent_dir)
                except:
                    pass  # Ignora se já existir
            
            with open(local_path, 'rb') as f:
                self.client.workspace.upload(workspace_path, f, overwrite=True)
            print("✅ Upload realizado!")
            return True
        except Exception as e:
            print(f"❌ Erro no upload: {e}")
            return False
    
    def test_connection(self):
        """Testa a conexão com o Databricks"""
        try:
            # Testar listagem de jobs para verificar conexão
            jobs_list = list(self.client.jobs.list(limit=5))
            print("✅ Conexão bem-sucedida!")
            return True
        except Exception as e:
            print(f"❌ Erro de conexão: {e}")
            return False