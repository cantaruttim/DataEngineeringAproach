from utils.utils import *
from pyspark.sql import SparkSession

# CONSTANT VARIABLES
BASE_FOLDER = Path("cvm_data")
ENTITY_NAME = "CIA_ABERTA"
FILE_NAME = "dfp_cia_aberta_2021.zip"
BASE_URL = "https://dados.cvm.gov.br/dados"
SPECIFIC_URL = "/CIA_ABERTA/DOC/DFP/DADOS/"

ACRONYMS_CIA = {
    "dre": "DRE",
    "bpa": "BPA",
    "dfc": "DFC",
    "dmpl": "DMPL",
    "dfp": "DFP",
    "inf": "INF"
}

spark = (SparkSession.builder.getOrCreate())

download_and_extract_zip(
    FILE_NAME,
    BASE_URL,
    SPECIFIC_URL,
    BASE_FOLDER
)

year = extract_year_from_filename(FILE_NAME)
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
