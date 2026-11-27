# Generated from the matching src notebook by job_deploy.sh.
# Edit the notebook in src, then regenerate this production script.
# Source: src/AN/bronze/an_liste_deputes_csv.ipynb

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

# Configuration des chemins
from lib.config import AN_BRONZE


# %%
url_dep = 'https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_csv_opendata/liste_deputes_excel.csv'
df = pd.read_csv(url_dep, sep = ";", encoding="cp1252")
df.head()

# %%
print("Présence de \\n :", df.astype(str).apply(lambda col: col.str.contains("\n")).any().any())
print("Présence de \\r :", df.astype(str).apply(lambda col: col.str.contains("\r")).any().any())

# %%
output_file = AN_BRONZE / "an_liste_deputes.parquet"

output_file.parent.mkdir(parents=True, exist_ok=True)

df.to_parquet(output_file, index=False)

# %%

# %%

# %%

# %%

