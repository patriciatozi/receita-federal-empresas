import requests
import zipfile
import io
import re
from bs4 import BeautifulSoup

def get_response(url):
    response = requests.get(url)
    response.raise_for_status()
    return response

def get_compiled_pattern(response, pattern):
    soup = BeautifulSoup(response.text, "html.parser")
    return re.compile(pattern), soup

def get_last_folder(url):
    response = get_response(url)
    pattern, soup = get_compiled_pattern(response, r"^\d{4}-\d{2}/$")
    folders = [a["href"].strip("/") for a in soup.find_all("a", href=pattern)]
    last_folder = sorted(folders)[-1]
    print("Última pasta encontrada:", last_folder)
    return last_folder

def get_source_data(endpoint, key, schema):
    last_updated_folder = get_last_folder(endpoint)
    response_folder = get_response(endpoint + last_updated_folder + "/")

    key_pattern, soup_folder = get_compiled_pattern(response_folder, r"^{key}\d+\.zip$".format(key=key))
    files = [a["href"] for a in soup_folder.find_all("a", href=key_pattern)]
    first_file = sorted(files, key=lambda x: int(re.search(r"(\d+)", x).group()))[0]
    print("Primeiro arquivo encontrado:", first_file)

    last_file = get_response(endpoint + last_updated_folder + "/" + first_file)
    z = zipfile.ZipFile(io.BytesIO(last_file.content))

    csv_name = z.namelist()[0]
    print("Arquivo dentro do ZIP:", csv_name)

    # extrai o arquivo em memória e lê com Spark
    with z.open(csv_name) as f:
        data = f.read()
        rdd = spark.sparkContext.parallelize(data.decode("latin1").splitlines())
        df = spark.read.csv(rdd, sep=";", schema=schema, header=False)

    print("Total de linhas:", df.count())
    return df