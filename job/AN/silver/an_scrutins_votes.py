# Generated from the matching src notebook by job_deploy.sh.
# Edit the notebook in src, then regenerate this production script.
# Source: src/AN/silver/an_scrutins_votes.ipynb

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
from lib.config import AN_SILVER

# %%
url_votes = "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip"

response = requests.get(url_votes)
response.raise_for_status()

zip_file = zipfile.ZipFile(io.BytesIO(response.content))

print(len(zip_file.namelist()), "fichiers dans le ZIP")

# %%
zip_file.namelist()[:5]

# %%
scrutins = []
votes_individuels = []

# %%
for fichier in zip_file.namelist():

    if not fichier.endswith(".json"):
        continue

    with zip_file.open(fichier) as f:
        data = json.load(f)

    scrutin = data["scrutin"]

    uid_scrutin = scrutin["uid"]

    # -------------------------
    # TABLE SCRUTINS
    # -------------------------

    synthese = scrutin.get("syntheseVote", {})
    decompte = synthese.get("decompte", {})

    scrutins.append({
        "uid": uid_scrutin,
        "numero": scrutin.get("numero"),
        "legislature": scrutin.get("legislature"),
        "date_scrutin": scrutin.get("dateScrutin"),
        "type_vote": scrutin.get("typeVote", {}).get("libelleTypeVote"),
        "sort": scrutin.get("sort", {}).get("code"),
        "titre": scrutin.get("titre"),

        "nombre_votants": synthese.get("nombreVotants"),
        "suffrages_exprimes": synthese.get("suffragesExprimes"),

        "pour": decompte.get("pour"),
        "contre": decompte.get("contre"),
        "abstentions": decompte.get("abstentions"),
        "non_votants": decompte.get("nonVotants")
    })

    # -------------------------
    # TABLE VOTES INDIVIDUELS
    # -------------------------

    ventilation = scrutin.get("ventilationVotes", {})
    organe = ventilation.get("organe", {})
    groupes = organe.get("groupes", {}).get("groupe", [])

    # Un seul groupe peut éventuellement être représenté par un dict
    if isinstance(groupes, dict):
        groupes = [groupes]

    for groupe in groupes:

        organe_groupe = groupe.get("organeRef")

        vote = groupe.get("vote", {})
        nominatif = vote.get("decompteNominatif", {})

        positions = {
            "pour": nominatif.get("pours"),
            "contre": nominatif.get("contres"),
            "abstention": nominatif.get("abstentions"),
            "non_votant": nominatif.get("nonVotants")
        }

        for position, bloc in positions.items():

            if bloc is None:
                continue

            votants = bloc.get("votant")

            if votants is None:
                continue

            # Un seul votant = dict
            if isinstance(votants, dict):
                votants = [votants]

            for votant in votants:

                votes_individuels.append({
                    "uid_scrutin": uid_scrutin,
                    "organe_groupe": organe_groupe,
                    "acteur_ref": votant.get("acteurRef"),
                    "mandat_ref": votant.get("mandatRef"),
                    "position": position,
                    "par_delegation": votant.get("parDelegation"),
                    "num_place": votant.get("numPlace"),
                    "cause_position_vote": votant.get("causePositionVote")
                })

# %%
df_scrutins = pd.DataFrame(scrutins)

df_votes_individuels = pd.DataFrame(votes_individuels)

# %%
df_scrutins.head()

# %%
df_votes_individuels.head()

# %%
output_file_scrutins = AN_SILVER / "scrutins.parquet"
output_file_scrutins.parent.mkdir(parents=True, exist_ok=True)

df_scrutins.to_parquet(output_file_scrutins, index=False)

print(f"Fichier enregistré : {output_file_scrutins}")

# %%
output_file_votes_individuels = AN_SILVER / "votes_individuels.parquet"
output_file_votes_individuels.parent.mkdir(parents=True, exist_ok=True)

df_votes_individuels.to_parquet(output_file_votes_individuels, index=False)

print(f"Fichier enregistré : {output_file_votes_individuels}")

