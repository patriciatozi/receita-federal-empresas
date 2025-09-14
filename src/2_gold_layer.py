from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, sum

def process_gold():
    spark = SparkSession.builder.appName("CNPI-Gold-Layer").getOrCreate()
    
    # Ler da camada silver
    df_empresas = spark.read.format("delta").load("/dbfs/FileStore/tables/silver/empresas")
    df_socios = spark.read.format("delta").load("/dbfs/FileStore/tables/silver/socios")
    
    # Calcular métricas
    socios_por_empresa = df_socios.groupBy("cnpi").agg(
        count("*").alias("qtde_socios"),
        sum(when(col("documento_socio") == "99999999999", 1).otherwise(0)).alias("socios_estrangeiros")
    )
    
    # Juntar e transformar
    df_gold = df_empresas.join(socios_por_empresa, "cnpi", "left") \
        .withColumn("flag_socio_estrangeiro", 
                   when(col("socios_estrangeiros") > 0, lit(True)).otherwise(lit(False))) \
        .withColumn("doc_alvo",
                   when((col("cod_porte") == "03") & (col("qtde_socios") > 1), lit(True)).otherwise(lit(False))) \
        .select("cnpi", "qtde_socios", "flag_socio_estrangeiro", "doc_alvo")
    
    # Salvar na camada gold
    df_gold.write.format("delta").mode("overwrite").save("/dbfs/FileStore/tables/gold/empresas_consolidadas")
    
    # Criar tabela SQL
    spark.sql("""
        CREATE TABLE IF NOT EXISTS empresas_consolidadas
        USING DELTA
        LOCATION '/FileStore/tables/gold/empresas_consolidadas'
    """)
    
    print(f"Gold layer completed: {df_gold.count()} records")
    spark.stop()

if __name__ == "__main__":
    process_gold()