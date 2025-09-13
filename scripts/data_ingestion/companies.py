
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from utils import get_source_data

def get_empresas(endpoint):

    spark = (
        SparkSession.builder 
        .appName("EmpresasReceita")
        .master("local[*]")
        .getOrCreate()
    )

    endpoint = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

    schema = StructType([
        StructField("cnpj", StringType(), True),
        StructField("razao_social", StringType(), True),
        StructField("natureza_juridica", IntegerType(), True),
        StructField("qualificacao_responsavel", IntegerType(), True),
        StructField("capital_social", StringType(), True),
        StructField("cod_porte", StringType(), True)
    ])

    df_empresas = get_source_data(spark, endpoint, "Empresas", schema)

    (
        df_empresas.coalesce(1).write.mode("overwrite")
        .parquet("empresas")
    )

    return True