import requests
import zipfile
import io
import os

def download_and_extract_zip(url):
    """Download e extrai arquivo ZIP"""
    print(f"Downloading: {url}")
    response = requests.get(url)
    
    z = zipfile.ZipFile(io.BytesIO(response.content))

    csv_name = z.namelist()[0]
    print("Arquivo dentro do ZIP:", csv_name)
    
    return z

def get_df_from_zip_file(spark, zip_file, schema):

    csv_name = zip_file.namelist()[0]
    print("Arquivo dentro do ZIP:", csv_name)

    # extrai o arquivo em memória e lê com Spark
    with zip_file.open(csv_name) as f:
        data = f.read()
        rdd = spark.sparkContext.parallelize(data.decode("latin1").splitlines())
        df = spark.read.csv(rdd, sep=";", schema=schema, header=False)

    print("Total de linhas:", df.count())
    return df
