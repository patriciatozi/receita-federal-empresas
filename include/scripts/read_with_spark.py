from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

    input_path = "/usr/local/airflow/include/data.csv"
    output_path = "/opt/spark-data/output/result.parquet"

    df = spark.read.option("header", "true").csv(input_path)
    print(f"📊 Lidos {df.count()} registros")

    df.show(5, truncate=False)
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()