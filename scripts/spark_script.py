#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession

def main():
    try:
        spark = SparkSession.builder \
            .appName("SparkSubmitExample") \
            .getOrCreate()
        
        print("Spark session created successfully!")
        
        # Exemplo simples de operação
        data = [("Apple", 10), ("Banana", 20), ("Orange", 15)]
        df = spark.createDataFrame(data, ["Fruit", "Quantity"])
        
        print("DataFrame:")
        df.show()
        
        print(f"Total records: {df.count()}")
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())