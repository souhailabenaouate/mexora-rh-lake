# Mexora RH Lake — Data Lake & Analyse du Marché IT Maroc

## Description
Ce projet construit un Data Lake Bronze/Silver/Gold peuplé avec 5 000 offres d'emploi IT marocaines, et produit une analyse stratégique du marché pour guider la politique RH de Mexora.

## Structure du projet

```
mexora_rh_lake/
├── pipeline/
│   ├── bronze_ingestion.py      # Ingestion brute dans la zone Bronze
│   ├── silver_transform.py      # Nettoyage et standardisation → Silver
│   ├── silver_nlp.py            # Extraction de compétences depuis texte
│   └── gold_aggregation.py      # Calcul des agrégats → Gold
├── analysis/
│   └── analyse_marche_it_maroc.ipynb  # Notebook d'analyse DuckDB
├── data/
│   ├── offres_emploi_it_maroc.json
│   ├── referentiel_competences_it.json
│   ├── entreprises_it_maroc.csv
│   └── generate_data.py
├── data_lake_mexora_rh/
│   ├── bronze/                  # Données brutes partitionnées
│   ├── silver/                  # Données nettoyées (Parquet)
│   └── gold/                    # Tables analytiques (Parquet)
├── main.py                      # Orchestration du pipeline complet
├── requirements.txt
└── README.md
```

## Installation

```bash
pip install pandas pyarrow duckdb matplotlib seaborn plotly jupyter numpy
```

## Utilisation

### 1. Générer les données
```bash
cd data
python generate_data.py
cd ..
```

### 2. Lancer le pipeline complet
```bash
python main.py
```

### Ou étape par étape :
```bash
python pipeline/bronze_ingestion.py
python pipeline/silver_transform.py
python pipeline/silver_nlp.py
python pipeline/gold_aggregation.py
```

### 3. Lancer le notebook d'analyse
```bash
jupyter notebook analysis/analyse_marche_it_maroc.ipynb
```

## Architecture Data Lake

| Zone | Format | Description |
|------|--------|-------------|
| Bronze | JSON | Données brutes immuables, partitionnées par source/mois |
| Silver | Parquet | Données nettoyées, normalisées, enrichies |
| Gold | Parquet | Agrégats analytiques, KPIs, indicateurs marché |

## Stack technique

| Outil | Usage |
|-------|-------|
| Python 3.12+ | Pipeline de traitement |
| pandas | Manipulation de données |
| pyarrow | Lecture/écriture Parquet |
| DuckDB | Requêtes SQL sur Parquet |
| matplotlib/seaborn | Visualisations |
| Jupyter | Notebook d'analyse |

## Résultats

- **5 000 offres** d'emploi IT marocaines traitées
- **69 partitions** Bronze (3 sources × 23 mois)
- **12 profils** IT normalisés
- **5 tables Gold** pour l'analyse
- **Analyse stratégique** du marché IT marocain pour Mexora
