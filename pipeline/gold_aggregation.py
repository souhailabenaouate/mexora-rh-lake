"""
gold_aggregation.py
Construit les 5 tables Gold depuis les données Silver.
Utilise DuckDB pour les requêtes SQL directement sur les fichiers Parquet.
"""

import duckdb
import pandas as pd
from pathlib import Path


def construire_gold(data_lake_root: str):
    silver_offres = str(Path(data_lake_root) / 'silver' / 'offres_clean' / 'offres_clean.parquet')
    silver_comp   = str(Path(data_lake_root) / 'silver' / 'competences_extraites' / 'competences.parquet')
    gold_path     = Path(data_lake_root) / 'gold'
    gold_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ── Table Gold 1 : Top compétences par profil ──────────────────────────
    print("[GOLD] Construction top_competences...")
    df_top_comp = con.execute(f"""
        SELECT
            profil,
            famille,
            competence,
            COUNT(DISTINCT id_offre) AS nb_offres_mentionnent,
            ROUND(COUNT(DISTINCT id_offre) * 100.0 /
                (SELECT COUNT(DISTINCT id_offre) FROM '{silver_offres}'), 2) AS pct_offres_total,
            RANK() OVER (
                PARTITION BY profil
                ORDER BY COUNT(DISTINCT id_offre) DESC
            ) AS rang_dans_profil
        FROM '{silver_comp}'
        WHERE competence != 'non_détecté'
        GROUP BY profil, famille, competence
        ORDER BY profil, rang_dans_profil
    """).df()
    df_top_comp.to_parquet(gold_path / 'top_competences.parquet', index=False)
    print(f"[GOLD] top_competences.parquet : {len(df_top_comp)} lignes")

    # ── Table Gold 2 : Salaires par profil et ville ────────────────────────
    print("[GOLD] Construction salaires_par_profil...")
    df_salaires = con.execute(f"""
        SELECT
            profil_normalise        AS profil,
            ville_std               AS ville,
            type_contrat_std        AS type_contrat,
            COUNT(*)                AS nb_offres,
            COUNT(*) FILTER (WHERE salaire_connu = true) AS nb_offres_avec_salaire,
            ROUND(MEDIAN(salaire_median_mad) FILTER (WHERE salaire_connu = true), 0) AS salaire_median_mad,
            ROUND(AVG(salaire_median_mad)    FILTER (WHERE salaire_connu = true), 0) AS salaire_moyen_mad,
            ROUND(MIN(salaire_min_mad)       FILTER (WHERE salaire_connu = true), 0) AS salaire_min_observe,
            ROUND(MAX(salaire_max_mad)       FILTER (WHERE salaire_connu = true), 0) AS salaire_max_observe
        FROM '{silver_offres}'
        GROUP BY profil_normalise, ville_std, type_contrat_std
        HAVING COUNT(*) >= 3
        ORDER BY nb_offres DESC
    """).df()
    df_salaires.to_parquet(gold_path / 'salaires_par_profil.parquet', index=False)
    print(f"[GOLD] salaires_par_profil.parquet : {len(df_salaires)} lignes")

    # ── Table Gold 3 : Volume d'offres par ville et profil ─────────────────
    print("[GOLD] Construction offres_par_ville...")
    df_villes = con.execute(f"""
        SELECT
            ville_std               AS ville,
            region_admin,
            profil_normalise        AS profil,
            annee,
            mois,
            COUNT(*)                AS nb_offres,
            COUNT(*) FILTER (WHERE teletravail ILIKE '%télétravail%'
                              OR teletravail ILIKE '%remote%'
                              OR teletravail ILIKE '%hybride%') AS nb_offres_remote,
            ROUND(COUNT(*) FILTER (WHERE teletravail ILIKE '%télétravail%'
                              OR teletravail ILIKE '%remote%'
                              OR teletravail ILIKE '%hybride%') * 100.0
                  / NULLIF(COUNT(*), 0), 1) AS pct_remote
        FROM '{silver_offres}'
        GROUP BY ville_std, region_admin, profil_normalise, annee, mois
        ORDER BY nb_offres DESC
    """).df()
    df_villes.to_parquet(gold_path / 'offres_par_ville.parquet', index=False)
    print(f"[GOLD] offres_par_ville.parquet : {len(df_villes)} lignes")

    # ── Table Gold 4 : Entreprises les plus recruteurs ─────────────────────
    print("[GOLD] Construction entreprises_recruteurs...")
    df_entreprises = con.execute(f"""
        SELECT
            entreprise,
            ville_std                           AS ville,
            COUNT(*)                            AS nb_offres_publiees,
            COUNT(DISTINCT profil_normalise)    AS nb_profils_differents,
            ROUND(AVG(salaire_median_mad) FILTER (WHERE salaire_connu = true), 0) AS salaire_moyen_propose,
            MIN(CAST(date_publication AS VARCHAR)) AS premiere_offre,
            MAX(CAST(date_publication AS VARCHAR)) AS derniere_offre
        FROM '{silver_offres}'
        WHERE entreprise IS NOT NULL AND entreprise != ''
        GROUP BY entreprise, ville_std
        HAVING COUNT(*) >= 3
        ORDER BY nb_offres_publiees DESC
        LIMIT 100
    """).df()
    df_entreprises.to_parquet(gold_path / 'entreprises_recruteurs.parquet', index=False)
    print(f"[GOLD] entreprises_recruteurs.parquet : {len(df_entreprises)} lignes")

    # ── Table Gold 5 : Tendances mensuelles ───────────────────────────────
    print("[GOLD] Construction tendances_mensuelles...")
    df_tendances = con.execute(f"""
        SELECT
            annee,
            mois,
            profil_normalise                    AS profil,
            COUNT(*)                            AS nb_offres,
            ROUND(AVG(salaire_median_mad) FILTER (WHERE salaire_connu = true), 0) AS salaire_moyen_mois
        FROM '{silver_offres}'
        GROUP BY annee, mois, profil_normalise
        ORDER BY profil_normalise, annee, mois
    """).df()
    df_tendances.to_parquet(gold_path / 'tendances_mensuelles.parquet', index=False)
    print(f"[GOLD] tendances_mensuelles.parquet : {len(df_tendances)} lignes")

    con.close()
    print(f"\n[GOLD] 5 tables Gold construites dans {gold_path}")


if __name__ == "__main__":
    BASE_DIR       = Path(__file__).resolve().parent.parent
    DATA_LAKE_ROOT = BASE_DIR / "data_lake_mexora_rh"

    print("=" * 50)
    print("GOLD AGGREGATION — Mexora RH Lake")
    print("=" * 50)

    construire_gold(str(DATA_LAKE_ROOT))
    print("\nOK Gold Aggregation terminé !")