# Generated from the matching src notebook by job_deploy.sh.
# Edit the notebook in src, then regenerate this production script.
# Source: src/AN/bronze/an_scrutins_json_zip.ipynb

# %%
import pandas as pd
import seaborn as sns
import matplotlib
import requests
import zipfile
import json
import io
import glob
import os
import sys

from pathlib import Path

# Racine du repository data
PROJECT_ROOT = Path.cwd().resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Configuration du stockage local Windows
# os.environ["DATA_ROOT"] = str(PROJECT_ROOT / "storage")

# Configuration des chemins
from lib.config import AN_BRONZE

# %%
url = "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip"

response = requests.get(url)
response.raise_for_status()

zip_file = zipfile.ZipFile(io.BytesIO(response.content))

print(len(zip_file.namelist()), "fichiers dans le ZIP")

# %%
output_file = AN_BRONZE / "scrutins.json.zip"
output_file.parent.mkdir(parents=True, exist_ok=True)

output_file.write_bytes(response.content)

print(f"Fichier enregistré : {output_file}")

