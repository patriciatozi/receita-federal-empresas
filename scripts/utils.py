from psycopg2.extras import execute_values
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandera as pa
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

    """
    Faz uma requisição HTTP GET para uma URL e retorna a resposta.
    
    Args:
        url (str): URL para fazer a requisição
        
    Returns:
        requests.Response: Objeto de resposta da requisição
        
    Raises:
        requests.HTTPError: Se a requisição retornar status code de erro
    """

    response = requests.get(url)
    response.raise_for_status()

    return response

def get_compiled_pattern(response, pattern):

    """
    Parseia o HTML da resposta e compila um padrão regex.
    
    Args:
        response (requests.Response): Resposta HTTP com conteúdo HTML
        pattern (str): Padrão regex para compilar
        
    Returns:
        tuple: (regex_pattern, BeautifulSoup) - padrão compilado e objeto BeautifulSoup
    """

    soup = BeautifulSoup(response.text, "html.parser")

    return re.compile(pattern), soup

def get_last_folder(url):

    """
    Encontra a pasta mais recente no diretório FTP da Receita Federal.
    
    Args:
        url (str): URL do diretório principal
        
    Returns:
        str: Nome da pasta mais recente (formato YYYY-MM)
    """

    response = get_response(url)

    pattern, soup = get_compiled_pattern(response, r"^\d{4}-\d{2}/$")

    folders = [a["href"].strip("/") for a in soup.find_all("a", href=pattern)]
    last_folder = sorted(folders)[-1]

    print("Última pasta encontrada:", last_folder)

    return last_folder

def get_last_update():

    """
    Retorna a data atual para uso em metadados.
    
    Returns:
        datetime.date: Data atual
    """

    last_update = datetime.date.today()

    return last_update

def get_source_data(
    key,
    file_columns,
    file_dtypes,
    test_mode=False
):
    
    """
    Obtém dados da fonte da Receita Federal baseado no tipo de arquivo.
    
    Args:
        key (str): Tipo de arquivo ('Empresas', 'Socios', 'Estabelecimentos')
        file_columns (list): Lista de nomes de colunas para o DataFrame
        file_dtypes (dict): Dicionário com tipos de dados das colunas
        test_mode (bool): Modo teste (evita chamadas HTTP reais)
        
    Returns:
        pd.DataFrame: DataFrame com os dados processados
        
    Raises:
        Exception: Se houver erro no download ou processamento do arquivo
    """

    if test_mode:
        # Retorna dados de teste sem fazer HTTP
        return pd.DataFrame({
            'cnpj': ['12345678000195', '98765432000187'],
            'razao_social': ['Empresa Teste A', 'Empresa Teste B'],
            'last_update': '2025-09'
        })
    
    endpoint = os.environ.get("MAIN_ENDPOINT", "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/")

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
    Salva DataFrame no PostgreSQL de forma idempotente com suporte a upsert.
    
    Args:
        df (pd.DataFrame): DataFrame com dados a serem salvos
        table (str): Nome da tabela de destino
        columns_table (dict): Dicionário com definição de colunas {nome: tipo}
        conflict_cols (list, optional): Colunas para detecção de conflitos
        mode (str, optional): Modo de operação - "update" (upsert), "ignore" (skip), None (insert)
        
    Raises:
        Exception: Se houver erro na operação de banco de dados
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
        table (str): Nome da tabela
        columns (list, optional): Lista de colunas para selecionar
        filters (str, optional): Condição WHERE para filtrar dados
    
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

    """
    Verifica se existem registros duplicados no DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame para verificar duplicatas
        
    Returns:
        bool: True se não há duplicatas, False se há duplicatas
    """

    duplicates = df.duplicated(keep=False).sum()

    if duplicates > 0:
        print(f"Existem {duplicates} registros duplicados")
        return False
    else:
        print("Não existem CNPJs duplicados")
        return True
    
def get_database_connection():

    """
    Cria conexão com o PostgreSQL usando variáveis de ambiente.
    
    Returns:
        psycopg2.connection: Conexão com o banco de dados
        
    Raises:
        Exception: Se não conseguir conectar ao banco
    """

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

def save_df_to_parquet(path, df, partition_by):

    """
    Salva DataFrame em formato Parquet com particionamento.
    
    Args:
        path (str): Caminho onde salvar o arquivo
        df (pd.DataFrame): DataFrame a ser salvo
        partition_by (list): Lista de colunas para particionamento
        
    Raises:
        Exception: Se houver erro ao salvar o arquivo
    """

    try:

        df.to_parquet(f"{path}", engine="pyarrow", index=False, partition_cols=partition_by)

        print(f"✅ Arquivo gravado no diretório {path} com sucesso!!")

    except Exception as e:
        print(f"❌ Erro ao gravar o arquivo no diretório {path}:", e)
        raise e
    

def log_quality_metric(table_name, stage, metric_name, metric_value, status='ok'):

    """
    Registra métrica de qualidade de dados no PostgreSQL.
    
    Args:
        table_name (str): Nome da tabela avaliada
        stage (str): Estágio do pipeline (bronze, silver, gold)
        metric_name (str): Nome da métrica
        metric_value (float): Valor da métrica
        status (str): Status da métrica ('ok', 'warning', 'error')
        
    Raises:
        Exception: Se houver erro ao persistir a métrica
    """

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

    """
    Calcula métricas básicas de qualidade de dados.
    
    Args:
        df (pd.DataFrame): DataFrame para análise
        table_name (str): Nome da tabela
        stage (str): Estágio do pipeline
    """

    total_records = len(df)
    null_percentage = (df.isnull().sum().sum() / (total_records * len(df.columns))) * 100

    status = 'ok'
    if null_percentage > 20:
        status = 'error'
    elif null_percentage > 5:
        status = 'warning'
    
    log_quality_metric(table_name, stage, 'total_records', total_records)
    log_quality_metric(table_name, stage, 'null_percentage', null_percentage, status)

    duplicate_count = df.duplicated().sum()
    dup_status = 'ok'
    if duplicate_count > 0:
        dup_status = 'warning' if duplicate_count <= 10 else 'error'
    log_quality_metric(table_name, stage, 'duplicate_count', duplicate_count, dup_status)
    
    for column in ['cnpj', 'cod_porte', 'natureza_juridica']:
        if column in df.columns:
            col_null_pct = (df[column].isnull().sum() / total_records) * 100
            col_status = 'error' if col_null_pct > 10 else 'warning' if col_null_pct > 2 else 'ok'
            log_quality_metric(table_name, stage, f'null_pct_{column}', col_null_pct, col_status)


def validate_data_quality_table(df, table, stage, schema):

    """
    Executa validação completa de qualidade de dados usando Pandera.
    
    Args:
        df (pd.DataFrame): DataFrame para validação
        table (str): Nome da tabela
        stage (str): Estágio do pipeline
        schema (pandera.Schema): Schema de validação
        
    Raises:
        Exception: Se a validação falhar com detalhes dos erros
    """

    calculate_basic_metrics(df, table, stage)

    try:

        schema.validate(df, lazy=True)
        print(f"✅ Todos os checks passaram para {table}!")

        log_quality_metric(table, stage, "validation_success", 100, "ok")

    except pa.errors.SchemaErrors as err:
        failure_df = err.failure_cases
        print(f"❌ Data Quality falhou para {table}: {len(failure_df)} erros encontrados")

        error_count = len(err.failure_cases)
        error_pct = (error_count / len(df)) * 100 if len(df) > 0 else 0
        
        status = 'error' if error_pct > 10 else 'warning'
        log_quality_metric(table, stage, 'validation_errors', error_count, status)
        log_quality_metric(table, stage, 'error_percentage', error_pct, status)

        passed_checks = df.drop(failure_df["index"])
        print(f"\n✅ Checks que passaram para {table}: {len(passed_checks)} linhas")
        print(passed_checks.head(10))

        raise Exception(f"❌ Data Quality falhou para {table}, {len(failure_df)} erros encontrados.")