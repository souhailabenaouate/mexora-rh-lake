"""
main.py
Orchestration complète du pipeline Mexora RH Lake.
Lance les 4 étapes dans l'ordre : Bronze → Silver → NLP → Gold
"""

from pathlib import Path
import time

def main():
    BASE_DIR       = Path(__file__).resolve().parent
    DATA_LAKE_ROOT = BASE_DIR / "data_lake_mexora_rh"
    DATA_DIR       = BASE_DIR / "data"

    print("=" * 60)
    print("  MEXORA RH LAKE — Pipeline Complet")
    print("=" * 60)
    start_total = time.time()

    # ── Étape 1 : Bronze Ingestion ─────────────────────────────────────────
    print("\n[1/4] BRONZE INGESTION...")
    t = time.time()
    from pipeline.bronze_ingestion import ingerer_bronze
    ingerer_bronze(
        str(DATA_DIR / "offres_emploi_it_maroc.json"),
        str(DATA_LAKE_ROOT)
    )
    print(f"[1/4] Terminé en {time.time()-t:.1f}s")

    # ── Étape 2 : Silver Transform ─────────────────────────────────────────
    print("\n[2/4] SILVER TRANSFORM...")
    t = time.time()
    from pipeline.silver_transform import (
        charger_depuis_bronze, normaliser_villes, normaliser_titres,
        normaliser_contrats, normaliser_salaires, normaliser_experience,
        ajouter_dates, sauvegarder_silver
    )
    df = charger_depuis_bronze(str(DATA_LAKE_ROOT))
    df = normaliser_villes(df)
    df = normaliser_titres(df)
    df = normaliser_contrats(df)
    df = normaliser_salaires(df)
    df = normaliser_experience(df)
    df = ajouter_dates(df)
    sauvegarder_silver(df, str(DATA_LAKE_ROOT))
    print(f"[2/4] Terminé en {time.time()-t:.1f}s")

    # ── Étape 3 : Silver NLP ───────────────────────────────────────────────
    print("\n[3/4] SILVER NLP — Extraction compétences...")
    t = time.time()
    from pipeline.silver_nlp import extraire_competences, sauvegarder_competences
    import pandas as pd
    df_silver = pd.read_parquet(DATA_LAKE_ROOT / "silver" / "offres_clean" / "offres_clean.parquet")
    df_comp = extraire_competences(df_silver, str(DATA_DIR / "referentiel_competences_it.json"))
    sauvegarder_competences(df_comp, str(DATA_LAKE_ROOT))
    print(f"[3/4] Terminé en {time.time()-t:.1f}s")

    # ── Étape 4 : Gold Aggregation ─────────────────────────────────────────
    print("\n[4/4] GOLD AGGREGATION...")
    t = time.time()
    from pipeline.gold_aggregation import construire_gold
    construire_gold(str(DATA_LAKE_ROOT))
    print(f"[4/4] Terminé en {time.time()-t:.1f}s")

    print("\n" + "=" * 60)
    print(f"  PIPELINE TERMINÉ en {time.time()-start_total:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()