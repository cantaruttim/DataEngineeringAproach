import requests
import zipfile
import io
from pyspark.sql import SparkSession
from utils.utils import *

spark = (
    SparkSession.builder
        .appName("AnaliseCVM")
        .getOrCreate()
)

file_name = "dfp_cia_aberta_2025.zip"
base_url = "https://dados.cvm.gov.br/dados"
specific_url = "/CIA_ABERTA/DOC/DFP/DADOS/"


def read_cvm_cia_data(file_name, base_url, specific_url):
    url = f"{base_url}{specific_url}{file_name}"
    zip_path = "dados_cvm.zip"
    extract_path = "./cvm_data"

    print(f"Downloading files: {url} ...")
    response = requests.get(url)

    if response.status_code == 200:
        print(f"Status code: {response.status_code} ... ✅ ")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(extract_path)
            csv_filename = z.namelist()[0]
            print(f"Arquivo {csv_filename} extraído com sucesso!")
        
        df = (
            spark
                .read
                .format("csv") 
                .option("header", "true") 
                .option("sep", ";") 
                .option("inferSchema", "true") 
                .option("encoding", "ISO-8859-1") 
                .load(f"{extract_path}/{csv_filename}")
        )
        
        print(f"Total rows: {df.count()}")

    else:
        print(f"Status code: {response.status_code} ... ❌")
        print("Error while download file ...")
    
    return df

df = read_cvm_cia_data(file_name, base_url, specific_url)

df = (
    df.select(
        'CNPJ_CIA', 
        'DT_REFER', 
        'VERSAO', 
        'DENOM_CIA', 
        'CD_CVM', 
        'CATEG_DOC', 
        'ID_DOC', 
        'DT_RECEB'
    )
)

df.show()


