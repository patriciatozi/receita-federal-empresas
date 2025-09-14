import os

def check_structure():
    print("Verificando estrutura de arquivos no container...")
    
    base_path = '/app'
    if not os.path.exists(base_path):
        print("❌ Diretório /app não existe!")
        return False
    
    print("Estrutura de /app:")
    for root, dirs, files in os.walk(base_path):
        level = root.replace(base_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")
    
    # Verificar arquivos específicos
    required_files = [
        'src/0_bronze_layer.py',
        'src/1_silver_layer.py', 
        'src/2_gold_layer.py',
        'src/utils.py',
        'src/databricks_client.py',
        'scripts/run_pipeline.py'
    ]
    
    print("\nVerificando arquivos requeridos:")
    for file in required_files:
        full_path = f"/app/{file}"
        exists = os.path.exists(full_path)
        status = "✅" if exists else "❌"
        print(f"{status} {file}: {exists}")
    
    return True

if __name__ == "__main__":
    check_structure()