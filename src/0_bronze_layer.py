from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils import download_and_extract_zip, get_df_from_zip_file
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def process_bronze():
    spark = SparkSession.builder.appName("CNPI-Bronze-Layer").getOrCreate()
    
    # URLs dos dados
    url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-08"
    
    # Download e extração
    empresas_zip = download_and_extract_zip(f"{url}/Empresas0.zip")

    schema_empresas = StructType([
        StructField("cnpj", StringType(), True),
        StructField("razao_social", StringType(), True),
        StructField("natureza_juridica", IntegerType(), True),
        StructField("qualificacao_responsavel", IntegerType(), True),
        StructField("capital_social", StringType(), True),
        StructField("cod_porte", StringType(), True)
    ])

    df_empresas = get_df_from_zip_file(spark, empresas_zip, schema_empresas)
    df_empresas.write.format("delta").mode("overwrite").save("/dbfs/FileStore/tables/bronze/empresas")

    socios_zip = download_and_extract_zip(f"{url}/Socios0.zip")

    schema_socios = StructType([
        StructField("cnpj", StringType(), True),
        StructField("tipo_socio", IntegerType(), True),
        StructField("nome_socio", StringType(), True), 
        StructField("documento_socio", StringType(), True),
        StructField("codigo_qualificacao_socio", StringType(), True)
    ])

    df_socios = get_df_from_zip_file(spark, socios_zip, schema_socios)
    
    df_socios.write.format("delta").mode("overwrite").save("/dbfs/FileStore/tables/bronze/socios")
    
    print(f"Bronze layer completed: {df_empresas.count()} empresas, {df_socios.count()} socios")
    
    spark.stop()

if __name__ == "__main__":
    process_bronze()