from pathlib import Path
from pyspark.sql import SparkSession
from utils.utils import *

# CONSTANT VARIABLES
BASE_FOLDER = Path("cvm_data")
ENTITY_NAME = "CIA_ABERTA"
file_name = "dfp_cia_aberta_2020.zip"
base_url = "https://dados.cvm.gov.br/dados"
specific_url = "/CIA_ABERTA/DOC/DFP/DADOS/"

spark = (
    SparkSession.builder
        .appName("AnaliseCVM")
        .getOrCreate()
)

ACRONYMS_CIA = {
    "dre": "DRE",
    "bpa": "BPA",
    "dfc": "DFC",
    "dmpl": "DMPL",
    "dfp": "DFP",
    "inf": "INF"
}

download_and_extract_zip(
    file_name,
    base_url,
    specific_url,
    BASE_FOLDER
)

year = extract_year_from_filename(file_name)
year_dir = move_files_to_year_folder(BASE_FOLDER, year)

year_entity_dir = move_year_to_entity(
    BASE_FOLDER,
    ENTITY_NAME,
    year_dir
)

organize_by_acronyms(
    year_entity_dir,
    ACRONYMS_CIA
)
