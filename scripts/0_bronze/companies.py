import sys
import os

sys.path.insert(0,'/opt/airflow/scripts')

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

    df_empresas = (
        get_source_data(
            endpoint,
            "Empresas",
            company_columns,
            company_dtypes
        )
    )

    print(df_empresas.head())

    return df_empresas

if __name__ == "__main__":
    get_companies()