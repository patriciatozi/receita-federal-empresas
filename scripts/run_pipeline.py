import sys
import os
sys.path.append('/app')

from src.databricks_client import DatabricksClient

def main():
    print("Iniciando pipeline CNPI...")
    print(f"Usando Serverless Starter Warehouse: 5e2035d2a5985127")
    
    client = DatabricksClient()
    
    # Testar conexão primeiro
    if not client.test_connection():
        print("❌ Falha na conexão com Databricks")
        return False
    
    # Upload dos scripts
    scripts_to_upload = [
        ('src/0_bronze_layer.py', '/scripts/bronze_layer.py'),
        ('src/1_silver_layer.py', '/scripts/silver_layer.py'),
        ('src/2_gold_layer.py', '/scripts/gold_layer.py'),
        ('src/utils.py', '/scripts/utils.py')
    ]
    
    for local_path, workspace_path in scripts_to_upload:
        print(f"📤 Uploading {local_path} to {workspace_path}")
        if not client.upload_file(local_path, workspace_path):
            print("❌ Falha no upload, continuando...")
    
    # Executar pipeline em sequência
    stages = [
        ('/scripts/bronze_layer.py', 'Bronze Layer'),
        ('/scripts/silver_layer.py', 'Silver Layer'),
        ('/scripts/gold_layer.py', 'Gold Layer')
    ]
    
    for script_path, stage_name in stages:
        print(f"\n=== Executing {stage_name} ===")
        run_id = client.submit_python_job(script_path)
        
        if run_id:
            print(f"✅ Job submitted with ID: {run_id}")
            success = client.wait_for_completion(run_id)
            if success:
                print(f"✅ {stage_name} completed successfully!")
            else:
                print(f"❌ {stage_name} failed!")
                return False
        else:
            print(f"❌ Failed to submit {stage_name}")
            return False
    
    print("\n🎉 Pipeline completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)