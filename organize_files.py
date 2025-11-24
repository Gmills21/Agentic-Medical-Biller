"""Script to organize project files into folders for GitHub."""
import os
import shutil
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Define folder structure
FOLDERS = {
    'medicare': [
        'medicare_price_calculator.py',
        'create_database.py',
        'test_calculator.py',
        'SYSTEM_ARCHITECTURE.md',
        'HOW_TO_TEST.md',
        'medicare.db',
        'national_county.txt',
        'Medicare CSVS',
    ],
    'ncci': [
        'check_ncci.py',
        'create_ncci_database.py',
        'NCCI.db',
    ],
    'scripts': [
        'check_db.py',
        'check_mod_indicators.py',
        'check_columns.py',
        'find_test_codes.py',
        'investigate_mod_indicator.py',
        'show_ptp_structure.py',
        'verify_db.py',
    ]
}

# Create folders
for folder_name in FOLDERS.keys():
    folder_path = BASE_DIR / folder_name
    # Remove if it's a file, then create as directory
    if folder_path.exists() and folder_path.is_file():
        folder_path.unlink()
        print(f"Removed file {folder_name}, creating directory")
    folder_path.mkdir(exist_ok=True)
    print(f"Created/verified folder: {folder_name}/")

# Move files
for folder_name, files in FOLDERS.items():
    folder_path = BASE_DIR / folder_name
    for file_name in files:
        source = BASE_DIR / file_name
        dest = folder_path / file_name
        
        if source.exists():
            if source.is_dir():
                # Move directory
                if dest.exists():
                    print(f"  Skipping {file_name} (already exists in {folder_name}/)")
                else:
                    shutil.move(str(source), str(dest))
                    print(f"  Moved {file_name}/ -> {folder_name}/")
            else:
                # Move file
                if dest.exists():
                    print(f"  Skipping {file_name} (already exists in {folder_name}/)")
                else:
                    shutil.move(str(source), str(dest))
                    print(f"  Moved {file_name} -> {folder_name}/")
        else:
            print(f"  Warning: {file_name} not found")

# Handle NCCI data folder separately (it's already in ncci folder)
ncci_data_source = BASE_DIR / 'NCCI'
if ncci_data_source.exists() and ncci_data_source.is_dir():
    ncci_folder = BASE_DIR / 'ncci'
    # Move files from NCCI to ncci if not already there
    for item in ncci_data_source.iterdir():
        if item.name not in ['check_ncci.py', 'create_ncci_database.py', 'NCCI.db']:
            dest = ncci_folder / item.name
            if not dest.exists():
                if item.is_dir():
                    shutil.move(str(item), str(dest))
                    print(f"  Moved {item.name}/ -> ncci/")
                else:
                    shutil.move(str(item), str(dest))
                    print(f"  Moved {item.name} -> ncci/")
    
    # Remove empty NCCI folder
    try:
        if not any(ncci_data_source.iterdir()):
            ncci_data_source.rmdir()
            print(f"  Removed empty NCCI/ folder")
    except:
        pass

print("\nOrganization complete!")

