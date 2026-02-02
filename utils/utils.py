from pathlib import Path
import requests
import zipfile
import io

# ================================
# ===== TREATING CNPJ OR CPF =====
# ================================




# ================================
# ===== EXTRACTING YEAR FROM =====
# ================================

def extract_year_from_filename(filename: str) -> str:
    return filename[-8:-4]

# =====================================
# ===== DOWNLOAD AND EXTRACT FILE =====
# =====================================

def download_and_extract_zip(
    file_name: str,
    base_url: str,
    specific_url: str,
    extract_path: Path
) -> None:

    url = f"{base_url}{specific_url}{file_name}"
    print(f"Downloading: {url} ... ")

    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(extract_path)

    print("Extraction completed! ✅ ")


# =========================================
# ===== MOVING FILES TO ENTITY FOLDER =====
# =========================================

def move_files_to_year_folder(
    base_folder: Path,
    year: str
) -> Path:

    year_dir = base_folder / year
    year_dir.mkdir(exist_ok=True)

    for item in base_folder.iterdir():
        if item.is_file() and year in item.name:
            print(f"Moving {item.name} → {year}/ ✅ ")
            item.rename(year_dir / item.name)

    return year_dir

def move_year_to_entity(
    base_folder: Path,
    entity_name: str,
    year_dir: Path
) -> Path:

    entity_dir = base_folder / entity_name
    entity_dir.mkdir(exist_ok=True)

    target_dir = entity_dir / year_dir.name

    if not target_dir.exists():
        print(f"Moving {year_dir.name} → {entity_name}/ ✅ ")
        year_dir.rename(target_dir)

    return target_dir

def organize_by_cia_aberta_acronyms(
    year_entity_dir: Path,
    acronyms: dict
) -> None:

    for file in year_entity_dir.iterdir():
        if not file.is_file():
            continue

        filename = file.name.lower()

        for key, folder_name in acronyms.items():
            if key in filename:
                target_dir = year_entity_dir / folder_name
                target_dir.mkdir(exist_ok=True)

                print(f"Moving {file.name} → {folder_name}/ ✅ ")
                file.rename(target_dir / file.name)
                break

