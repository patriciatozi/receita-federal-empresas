from bs4 import BeautifulSoup
import pandas as pd
import requests
import zipfile
import re
import io
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

# Carrega variáveis do .env
load_dotenv()

def get_response(url):

    response = requests.get(url)
    response.raise_for_status()

    return response

def get_compiled_pattern(response, pattern):

    soup = BeautifulSoup(response.text, "html.parser")

    return re.compile(pattern), soup

def get_last_folder(url):

    response = get_response(url)

    pattern, soup = get_compiled_pattern(response, r"^\d{4}-\d{2}/$")

    folders = [a["href"].strip("/") for a in soup.find_all("a", href=pattern)]
    last_folder = sorted(folders)[-1]

    print("Última pasta encontrada:", last_folder)

    return last_folder

def get_source_data(
    endpoint,
    key,
    file_columns,
    file_dtypes
):

    last_updated_folder = get_last_folder(endpoint)

    response_folder = get_response(endpoint + last_updated_folder + "/")

    key_pattern, soup_folder = get_compiled_pattern(response_folder, r"^{key}\d+\.zip$".format(key=key))

    files = [a["href"] for a in soup_folder.find_all("a", href=key_pattern)]
    last_file = sorted(files, key=lambda x: int(re.search(r"(\d+)", x).group()))[1]
    print("Primeiro arquivo encontrado:", last_file)

    last_file = get_response(endpoint + last_updated_folder + "/" + last_file)

    z = zipfile.ZipFile(io.BytesIO(last_file.content))

    csv_name = z.namelist()[0]  # pega o primeiro arquivo dentro do zip
    print("Arquivo dentro do ZIP:", csv_name)

    df = pd.read_csv(
        z.open(csv_name),
        sep=";",
        encoding="latin1",
        usecols=list(range(len(file_columns))),
        names=file_columns,
        dtype=file_dtypes,
        header=None,
        low_memory=False
    )

    print("Total de linhas:", len(df))

    return df

def save_to_postgres(df, columns_table, db_config, conflict_cols=None, mode="update"):
    """
    Salva DataFrame no Postgres de forma idempotente.

    Args:
        df (pd.DataFrame): Dados a serem salvos.
        columns_table (dict): Colunas e tipos para criação da tabela.
        db_config (dict): Credenciais do banco + nome da tabela.
        conflict_cols (list): Colunas que definem unicidade (chave natural).
        mode (str): "update" para upsert, "ignore" para ignorar duplicados, None para sempre inserir.
    """

    # 1. Substituir <NA> (pd.NA / NAType) e NaN por None (compatível com psycopg2/Postgres)
    df = df.astype(object).where(pd.notna(df), None)

    conn = psycopg2.connect(
        host=db_config["host"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        port=db_config["port"],
    )
    cur = conn.cursor()

    table = db_config["table"]

    # 2. Criar tabela se não existir
    col_defs = [f"{col} {dtype}" for col, dtype in columns_table.items()]
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
    cur.execute(create_table_sql)

    # 3. Garantir constraints para ON CONFLICT
    if conflict_cols:
        if len(conflict_cols) == 1:
            col = conflict_cols[0]
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints
                        WHERE table_name = '{table}'
                          AND constraint_type = 'PRIMARY KEY'
                    ) THEN
                        ALTER TABLE {table} ADD PRIMARY KEY ({col});
                    END IF;
                END;
                $$;
            """)
        else:
            constraint_name = f"{table}_{'_'.join(conflict_cols)}_uniq"
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = '{constraint_name}'
                    ) THEN
                        ALTER TABLE {table}
                        ADD CONSTRAINT {constraint_name} UNIQUE ({', '.join(conflict_cols)});
                    END IF;
                END;
                $$;
            """)

    conn.commit()

    # 4. Preparar dados para insert
    cols = list(columns_table.keys())
    values = [tuple(row[col] for col in cols) for _, row in df.iterrows()]

    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"

    if conflict_cols and mode == "update":
        update_assignments = ", ".join(
            [f"{col}=EXCLUDED.{col}" for col in cols if col not in conflict_cols]
        )
        insert_sql += f" ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {update_assignments}"
    elif conflict_cols and mode == "ignore":
        insert_sql += f" ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"

    # 5. Executar inserção
    try:
        execute_values(cur, insert_sql, values)
        conn.commit()
        print(f"✅ {len(values)} registros inseridos em {table} (modo={mode})!")
    except Exception as e:
        print("❌ Erro ao inserir os dados:", e)
        raise e
    finally:
        cur.close()
        conn.close()

def read_table(db_config, table, columns=None, filters=None):
    """
    Lê uma tabela do PostgreSQL e retorna um DataFrame.

    Args:
        db_config (dict): Dicionário com chaves 'host', 'port', 'dbname', 'user', 'password'.
        table (str): Nome da tabela a ser lida.
        columns (list, opcional): Lista de colunas a selecionar. Se None, seleciona todas.
        filters (str, opcional): Condições SQL adicionais (ex: "cnpj='12345678'").
    
    Returns:
        pd.DataFrame: DataFrame com os dados da tabela.
    """
    # Define colunas
    cols_sql = ", ".join(columns) if columns else "*"
    
    # Monta query
    query = f"SELECT {cols_sql} FROM {table}"
    if filters:
        query += f" WHERE {filters}"
    
    # Conecta ao banco
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config["port"],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )
    
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def check_for_duplicates(df):

    duplicates = df.duplicated(keep=False).sum()

    if duplicates > 0:
        print(f"Existem {duplicates} registros duplicados")
        return False
    else:
        print("Não existem CNPJs duplicados")
        return True
    
def get_database_engine():
    """Cria engine de conexão com o PostgreSQL usando variáveis de ambiente"""
    db_config = {
        'drivername': 'postgresql',
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'username': os.getenv('POSTGRES_USER', 'airflow'),
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
        'database': os.getenv('POSTGRES_DB', 'airflow')
    }
    
    # Cria a URL de conexão
    db_url = URL.create(**db_config)
    
    # Cria o engine
    engine = create_engine(db_url)
    
    return engine

def get_connection_string():
    """Retorna a string de conexão formatada"""
    return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"