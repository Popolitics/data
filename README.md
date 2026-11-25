# data

Pipelines ETL, ingestion et transformation des données publiques via Kestra.

## Setup

```bash
cp .env.example .env    # générer un vrai mot de passe, ex. openssl rand -base64 24
docker compose up -d
```

Ouvrir [http://localhost:8080](http://localhost:8080) — interface Kestra.
Premier lancement : Kestra demande de créer un compte admin local (email +
mot de passe), propre à chaque instance — rien à configurer à l'avance.

Le port n'est exposé qu'en loopback (`127.0.0.1:8080`), volontairement — pas
question de rendre Kestra accessible sur le réseau sans authentification
réseau en plus.

Les flows (déclarations YAML des pipelines) vivent dans `flows/`, montés
automatiquement dans le conteneur — pas besoin de redémarrer pour qu'un
nouveau flow soit pris en compte.

```bash
docker compose down        # arrêter
docker compose down -v     # arrêter + effacer les données locales
```

## Statut

Kestra tourne, mais aucun flow Kestra n'est encore écrit. Le pipeline AN des
comptes rendus ci-dessous est actuellement exécuté manuellement dans Jupyter.

## Générer les scripts de production

`src/` contient les notebooks de développement ; `job/` contient les scripts
Python destinés à l'orchestration Kestra. Depuis la racine du dépôt, lancer :

```bash
bash job_deploy.sh
```

Le script convertit chaque `src/**/*.ipynb` en `.py` sous `job/`, en
reproduisant son chemin relatif. Il conserve les cellules Markdown sous forme
de commentaires, ajoute des séparateurs `# %%`, vérifie la syntaxe de toutes
les sorties avant de les écrire et refuse les magics IPython non exportables.
Il n'a besoin que de Python 3 et ne supprime pas les anciens fichiers `.py`
de `job/`.

## ETL AN — comptes rendus de séance (données préparatoires au RAG)

Le pipeline suit les couches Bronze / Silver. Le code de
[`bronze_silver_RAG.py`](src/AN/bronze/bronze_silver_RAG.py) est conservé sans
modification et recopié dans le notebook Silver : ce fichier est ignoré par
Git, le notebook ne l'importe donc pas. Si le parseur évolue, mettre à jour
également sa copie dans le notebook.

### Ordre d'exécution

1. [`an_cr_seances_fetch.ipynb`](src/AN/bronze/an_cr_seances_fetch.ipynb) —
   télécharge l'archive XML officielle de l'Assemblée nationale, la conserve
   telle quelle en Bronze et extrait les XML sans les réécrire.
2. [`an_cr_seances_transform.ipynb`](src/AN/silver/an_cr_seances_transform.ipynb) —
   exécute la copie du code source, ignore les comptes rendus provisoires et
   produit la sortie du CLI sélectionnée par `OUTPUT_FORMAT`. La valeur par
   défaut `json` correspond au mode JSON du script ; `tables` sélectionne son
   mode tables.

### Sorties

Les notebooks utilisent `lib.config` et écrivent sous `DATA_ROOT` (valeur par
défaut : `/data`) :

```text
AN/bronze/cr_seances/
├── syseron.xml.zip             # archive source originale
├── manifest.json               # source, checksum et liste des fichiers
└── xml/                         # XML extraits sans transformation
AN/silver/cr_seances/
├── seances/                     # sortie choisie si OUTPUT_FORMAT = 'json'
└── tables/                      # sortie choisie si OUTPUT_FORMAT = 'tables'
```

Le parseur classe notamment les interventions, votes, points et élections ;
ses sorties contiennent le texte et les métadonnées utiles à un traitement
RAG ultérieur. Le code source ne découpe pas les textes en chunks, ne calcule
pas d'embeddings et n'insère rien dans PostgreSQL/pgvector.

Les notebooks cherchent la racine du dépôt depuis leur répertoire courant ;
une version convertie en `.py` la cherche d'abord depuis `__file__`, donc
depuis son emplacement sous `job`. Dans les deux cas, les données sont
stockées via `lib.config` sous `DATA_ROOT` (défaut : `/data`), pas à côté du
script. En déploiement, monter `job` et le répertoire de données dans
l'environnement d'exécution et définir `DATA_ROOT` vers ce montage. Le
`docker-compose.yml` actuel ne monte pas encore `job` ni `/data`, et aucun
flow Kestra ne lance encore ces scripts.

Le téléchargement requiert `requests`, déjà déclaré dans `pyproject.toml`.
