from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG


def test_postgres_connection():
    """Testa a conexão com PostgreSQL"""
    try:
        print("🔌 Testando conexão com PostgreSQL...")
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ PostgreSQL Version: {version[0]}")
        
        cursor.execute("SELECT datname FROM pg_database;")
        databases = cursor.fetchall()
        print("📊 Databases disponíveis:")
        for db in databases:
            print(f"  - {db[0]}")
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)

        tables = cursor.fetchall()

        print("📋 Tabelas no database 'airflow':")

        for table in tables:
            print(f"  - {table[0]}")
        
        conn.close()

        print("✅ Conexão testada com sucesso!")
        
    except Exception as e:

        print(f"❌ Erro na conexão: {e}")
        print("💡 Verifique se a connection 'postgres_default' está configurada")

        raise

with DAG(
    'test_postgres_connection',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    test_connection_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )

    test_connection_task