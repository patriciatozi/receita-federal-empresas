# apps/spark_watcher.py
import time
import os
import subprocess
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

WATCH_DIR = "/opt/spark-apps/triggers"
PROCESSED_DIR = "/opt/spark-apps/processed"

def process_trigger_files():
    """Monitorar diretório e processar arquivos trigger"""
    
    # Criar diretórios se não existirem
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    logger.info(f"Starting spark watcher. Watching directory: {WATCH_DIR}")
    
    while True:
        try:
            for filename in os.listdir(WATCH_DIR):
                if filename.endswith('.trigger'):
                    trigger_path = os.path.join(WATCH_DIR, filename)
                    
                    logger.info(f"Processing trigger file: {filename}")
                    
                    # Executar o job Spark
                    try:
                        result = subprocess.run([
                            "/opt/bitnami/spark/bin/spark-submit",
                            "--master", "spark://spark-master:7077",
                            "/opt/spark-apps/read.py"
                        ], capture_output=True, text=True, timeout=300)
                        
                        logger.info(f"Spark job completed with return code: {result.returncode}")
                        if result.stdout:
                            logger.info(f"STDOUT: {result.stdout}")
                        if result.stderr:
                            logger.info(f"STDERR: {result.stderr}")
                            
                    except subprocess.TimeoutExpired:
                        logger.error("Spark job timed out after 5 minutes")
                    except Exception as e:
                        logger.error(f"Error executing spark job: {e}")
                    
                    # Mover arquivo para processed
                    processed_path = os.path.join(PROCESSED_DIR, filename)
                    os.rename(trigger_path, processed_path)
                    logger.info(f"Moved {filename} to processed directory")
            
            # Esperar antes de verificar novamente
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Error in watcher: {e}")
            time.sleep(30)

if __name__ == "__main__":
    process_trigger_files()