from pathlib import Path
import os

folder = "./cvm_data"
file_name = "dfp_cia_aberta_2025.zip"

def extract_year_from_file(file_name : str):
    return file_name[-8:-4]

year = extract_year_from_file(file_name)

if year != exit:
    os.mkdir(folder + "/" + year)
    


# def read_folder(folder):
#     pasta = Path(folder)

#     for item in pasta.iterdir():
#         print(item.name)

# read_folder(folder)