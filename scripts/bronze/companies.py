import sys
import os
import psycopg2

sys.path.insert(0, '/opt/airflow/scripts')

from utils import get_source_data

def get_companies():
    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

    company_columns = [
        "cnpj",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "cod_porte"
    ]

    company_dtypes = {
        "cnpj": "string",
        "razao_social": "string",
        "natureza_juridica": "Int64", 
        "qualificacao_responsavel": "Int64",
        "capital_social": "string",
        "cod_porte": "string"
    }

    df_empresas = get_source_data(
        endpoint,
        "Empresas",
        company_columns,
        company_dtypes
    )

    print(f"✅ Total de empresas extraídas: {len(df_empresas):,}")
    print(df_empresas.head())

    return df_empresas


def save_to_postgres(df):
    """Insere os dados extraídos na tabela companies"""
    conn = psycopg2.connect(
        host="postgres",      # nome do serviço do docker-compose
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cur = conn.cursor()

    # Cria a tabela se não existir
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            cnpj TEXT,
            razao_social TEXT,
            natureza_juridica BIGINT,
            qualificacao_responsavel BIGINT,
            capital_social TEXT,
            cod_porte TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Insere os dados linha a linha
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO companies (cnpj, razao_social, natureza_juridica,
                                   qualificacao_responsavel, capital_social, cod_porte)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            row["cnpj"],
            row["razao_social"],
            row["natureza_juridica"],
            row["qualificacao_responsavel"],
            row["capital_social"],
            row["cod_porte"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Dados salvos no Postgres com sucesso!")


def main():
    """Função principal para execução direta do script"""
    try:
        df = get_companies()
        save_to_postgres(df)
    except Exception as e:
        print(f"❌ Erro no script: {e}")
        raise


if __name__ == "__main__":
    main()