from psycopg2.extras import execute_values
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import requests
import datetime
import zipfile
import re
import io
import os
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

def get_last_update():

    last_update = datetime.date.today()

    return last_update

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

    csv_name = z.namelist()[0] 
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

    df['last_update'] = last_updated_folder

    print("Total de linhas:", len(df))

    return df

def save_to_postgres(df, table, columns_table, conflict_cols=None, mode="update"):
    """
    Salva DataFrame no Postgres de forma idempotente.

    Args:
        df (pd.DataFrame): Dados a serem salvos.
        columns_table (dict): Colunas e tipos para criação da tabela.
        db_config (dict): Credenciais do banco + nome da tabela.
        conflict_cols (list): Colunas que definem unicidade (chave natural).
        mode (str): "update" para upsert, "ignore" para ignorar duplicados, None para sempre inserir.
    """

    df = df.astype(object).where(pd.notna(df), None)

    conn = get_database_connection()
    cur = conn.cursor()

    col_defs = [f"{col} {dtype}" for col, dtype in columns_table.items()]
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
    cur.execute(create_table_sql)

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

def read_table(table, columns=None, filters=None):

    """
    Faz a leitura de uma tabela do PostgreSQL e retorna um DataFrame.

    Args:
        db_config (dict): Dicionário com chaves 'host', 'port', 'dbname', 'user', 'password'.
        table (str): Nome da tabela a ser lida.
        columns (list, opcional): Lista de colunas a selecionar. Se None, seleciona todas.
        filters (str, opcional): Condições SQL adicionais (ex: "cnpj='12345678'").
    
    Returns:
        pd.DataFrame: DataFrame com os dados da tabela.
    """

    cols_sql = ", ".join(columns) if columns else "*"

    query = f"SELECT {cols_sql} FROM {table}"
    if filters:
        query += f" WHERE {filters}"

    conn = get_database_connection()
    
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
    
def get_database_connection():

    """Cria a conexão com o PostgreSQL usando variáveis de ambiente"""

    db_config = {
            "host": os.environ["POSTGRES_HOST"],
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "port": int(os.environ["POSTGRES_PORT"]),
        }
    
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config["port"],
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password']
    )
    
    return conn

def get_connection_string():

    """Retorna a string de conexão formatada"""

    return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

def save_df_to_parquet(path, df, partition_by):

    try:

        df.to_parquet(f"{path}", engine="pyarrow", index=False, partition_cols=partition_by)

        print(f"✅ Arquivo gravado no diretório {path} com sucesso!!")

    except Exception as e:
        print(f"❌ Erro ao gravar o arquivo no diretório {path}:", e)
        raise e
    

def log_quality_metric(table_name, stage, metric_name, metric_value, status='ok'):

    """Log simples de métrica de qualidade"""

    try:
        metric_data = {
            'table_name': table_name,
            'stage': stage,
            'metric_name': metric_name,
            'metric_value': metric_value,
            'status': status
        }
        
        df = pd.DataFrame([metric_data])
        df['last_update'] = get_last_update()
        
        print(f"✓ Métrica {metric_name} para {table_name} ({stage}): {metric_value} [{status}]")

        metric_columns = {
            'table_name': 'TEXT',
            'stage': 'TEXT',
            'metric_name': 'TEXT',
            'metric_value': 'FLOAT',
            'status': 'TEXT',
            'last_update': 'DATE'
        }

        save_to_postgres(df, "data_quality_metrics", metric_columns, conflict_cols=['table_name', 'metric_name', 'last_update'])
        
    except Exception as e:
        print(f"✗ Erro ao logar métrica: {e}")
        raise

def calculate_basic_metrics(df, table_name, stage):

    """Calcula métricas básicas de forma simples"""

    total_records = len(df)
    null_percentage = (df.isnull().sum().sum() / (total_records * len(df.columns))) * 100

    status = 'ok'
    if null_percentage > 20:
        status = 'error'
    elif null_percentage > 5:
        status = 'warning'
    
    log_quality_metric(table_name, stage, 'total_records', total_records)
    log_quality_metric(table_name, stage, 'null_percentage', null_percentage, status)
    
    for column in ['cnpj', 'cod_porte', 'natureza_juridica']:
        if column in df.columns:
            col_null_pct = (df[column].isnull().sum() / total_records) * 100
            col_status = 'error' if col_null_pct > 10 else 'warning' if col_null_pct > 2 else 'ok'
            log_quality_metric(table_name, stage, f'null_pct_{column}', col_null_pct, col_status)