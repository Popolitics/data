# data

Pipelines ETL, ingestion et transformation des données publiques via Kestra.

## Setup

```bash
docker compose up -d
```

Ouvrir [http://localhost:8080](http://localhost:8080) — interface Kestra.
Premier lancement : Kestra demande de créer un compte admin local (email +
mot de passe), propre à chaque instance — rien à configurer à l'avance.

Les flows (déclarations YAML des pipelines) vivent dans `flows/`, montés
automatiquement dans le conteneur — pas besoin de redémarrer pour qu'un
nouveau flow soit pris en compte.

```bash
docker compose down        # arrêter
docker compose down -v     # arrêter + effacer les données locales
```

## Statut

Socle de base uniquement : Kestra tourne, aucun flow n'est encore écrit.
Restent à définir : la source de données (AN, Sénat, Parlement européen) par
pipeline, et où les données transformées atterrissent (base partagée avec
`data-service` ou base séparée — pas encore tranché).
