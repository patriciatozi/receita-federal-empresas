from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_silver():
    spark = SparkSession.builder.appName("CNPI-Silver-Layer").getOrCreate()
    
    # Ler da camada bronze
    df_empresas = spark.read.format("delta").load("/dbfs/FileStore/tables/bronze/empresas")
    df_socios = spark.read.format("delta").load("/dbfs/FileStore/tables/bronze/socios")
    
    # Transformar para schema silver
    empresas_silver = df_empresas.select(
        col("cnpj_basico").alias("cnpi"),
        col("razao_social"),
        col("natureza_juridica").cast("int"),
        col("qualificacao_do_responsavel").cast("int").alias("qualificacao_responsavel"),
        col("capital_social_da_empresa").cast("double").alias("capital_social"),
        col("porte_da_empresa").alias("cod_porte")
    )
    
    socios_silver = df_socios.select(
        col("cnpj_basico").alias("cnpi"),
        col("identificador_de_socio").cast("int").alias("tipo_socio"),
        col("nome_do_socio_ou_razao_social").alias("nome_socio"),
        col("cpf_ou_cnpj_do_socio").alias("documento_socio"),
        col("qualificacao_do_socio").alias("codigo_qualificacao_socio")
    )
    
    # Salvar na camada silver
    empresas_silver.write.format("delta").mode("overwrite").save("/dbfs/FileStore/tables/silver/empresas")
    socios_silver.write.format("delta").mode("overwrite").save("/dbfs/FileStore/tables/silver/socios")
    
    print("Silver layer completed")
    spark.stop()

if __name__ == "__main__":
    process_silver()