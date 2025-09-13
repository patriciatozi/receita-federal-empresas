# read.py
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()
    
    df = spark.read.csv("./include/data.csv", header="true")
    df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()