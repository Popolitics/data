# Generated from the matching src notebook by job_deploy.sh.
# Edit the notebook in src, then regenerate this production script.
# Source: src/AN/bronze/an_cr_seances_fetch.ipynb

# %% [markdown]
# # Bronze — récupération des comptes rendus XML
#
# Télécharge l'archive officielle de l'Assemblée nationale, la conserve sans modification dans Bronze et en extrait les XML octet pour octet. Aucune transformation métier n'est faite ici.
#
# Exécuter ensuite [la transformation Silver](../silver/an_cr_seances_transform.ipynb).

# %%
import hashlib
import json
import shutil
import sys
import zipfile
from pathlib import Path, PurePosixPath

import requests

CONFIG_FILE = "config.py"

cwd = Path.cwd().resolve()
locations = [cwd]
try:
    locations.insert(0, Path(__file__).resolve().parent)
except NameError:
    pass

PROJECT_ROOT = None
for location in locations:
    candidates = [location, *location.parents]
    PROJECT_ROOT = next((path for path in candidates if (path / 'lib' / CONFIG_FILE).is_file()), None)
    if PROJECT_ROOT is None:
        PROJECT_ROOT = next((path / 'data' for path in candidates if (path / 'data' / 'lib' / CONFIG_FILE).is_file()), None)
    if PROJECT_ROOT is None:
        for child in location.iterdir():
            if (child / 'lib' / CONFIG_FILE).is_file():
                PROJECT_ROOT = child
                break
            if (child / 'data' / 'lib' / CONFIG_FILE).is_file():
                PROJECT_ROOT = child / 'data'
                break
    if PROJECT_ROOT is not None:
        break
if PROJECT_ROOT is None:
    raise FileNotFoundError(f'Impossible de trouver la racine du dépôt data (lib/{CONFIG_FILE}).')

sys.path.insert(0, str(PROJECT_ROOT))
from lib.config import AN_BRONZE

# %%
SOURCE_URL = 'https://data.assemblee-nationale.fr/static/openData/repository/17/vp/syceronbrut/syseron.xml.zip'
BRONZE_DIR = AN_BRONZE / 'cr_seances'
ZIP_PATH = BRONZE_DIR / 'syseron.xml.zip'
XML_DIR = BRONZE_DIR / 'xml'
MANIFEST_PATH = BRONZE_DIR / 'manifest.json'

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
temporary_zip = ZIP_PATH.with_name(ZIP_PATH.name + '.part')
sha256 = hashlib.sha256()

try:
    with requests.get(SOURCE_URL, stream=True, timeout=(20, 600)) as response:
        response.raise_for_status()
        with temporary_zip.open('wb') as destination:
            for block in response.iter_content(chunk_size=1024 * 1024):
                if block:
                    destination.write(block)
                    sha256.update(block)
    if not zipfile.is_zipfile(temporary_zip):
        raise ValueError('La ressource téléchargée n’est pas une archive ZIP valide.')
    temporary_zip.replace(ZIP_PATH)
finally:
    if temporary_zip.exists():
        temporary_zip.unlink()

archive_sha256 = sha256.hexdigest()
print(f'Archive brute enregistrée : {ZIP_PATH}')
print(f'SHA-256 : {archive_sha256}')

# %%
XML_DIR.mkdir(parents=True, exist_ok=True)
xml_files = []
xml_root = XML_DIR.resolve()

with zipfile.ZipFile(ZIP_PATH) as archive:
    members = [item for item in archive.infolist() if not item.is_dir() and PurePosixPath(item.filename).suffix.lower() == '.xml']
    if not members:
        raise ValueError('Aucun fichier XML trouvé dans l’archive officielle.')

    for item in members:
        member_path = PurePosixPath(item.filename)
        if member_path.is_absolute() or '..' in member_path.parts or '\\' in item.filename:
            raise ValueError(f'Chemin invalide dans l’archive : {item.filename}')
        output_path = XML_DIR.joinpath(*member_path.parts).resolve()
        if xml_root not in output_path.parents:
            raise ValueError(f'Le fichier sortirait du répertoire Bronze : {item.filename}')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(item) as source, output_path.open('wb') as destination:
            shutil.copyfileobj(source, destination)
        xml_files.append(member_path.as_posix())

manifest = {
    'source_url': SOURCE_URL,
    'archive': ZIP_PATH.name,
    'archive_sha256': archive_sha256,
    'xml_files': sorted(xml_files),
}
MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'{len(xml_files)} XML extraits sans transformation dans {XML_DIR}')
print(f'Manifeste : {MANIFEST_PATH}')

