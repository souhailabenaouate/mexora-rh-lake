"""
silver_nlp.py
Extrait les compétences IT depuis le texte libre des offres.
Sources : champ 'competences_brut' + champ 'description'
Stratégie : matching sur le référentiel de compétences normalisé.
"""

import json
import re
import pandas as pd
from pathlib import Path


def extraire_competences(df: pd.DataFrame, referentiel_path: str) -> pd.DataFrame:
    print("[NLP] Chargement du référentiel de compétences...")
    with open(referentiel_path, 'r', encoding='utf-8') as f:
        referentiel = json.load(f)

    # Construire dictionnaire plat : alias → nom_normalise, famille
    dict_competences = {}
    for famille, competences in referentiel['familles'].items():
        for nom_normalise, aliases in competences.items():
            for alias in aliases:
                dict_competences[alias.lower()] = {
                    'competence': nom_normalise,
                    'famille': famille
                }

    # Trier par longueur décroissante pour éviter faux positifs
    aliases_tries = sorted(dict_competences.keys(), key=len, reverse=True)
    print(f"[NLP] {len(dict_competences)} aliases chargés depuis le référentiel")

    resultats = []
    nb_sans_competence = 0

    for _, offre in df.iterrows():
        # Concaténer les deux sources de texte
        texte_complet = ' '.join(filter(None, [
            str(offre.get('competences_brut', '') or ''),
            str(offre.get('description', '') or '')
        ])).lower()

        competences_trouvees = set()

        for alias in aliases_tries:
            pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, texte_complet):
                info = dict_competences[alias]
                cle = info['competence']
                if cle not in competences_trouvees:
                    competences_trouvees.add(cle)
                    resultats.append({
                        'id_offre':   offre['id_offre'],
                        'profil':     offre.get('profil_normalise'),
                        'ville':      offre.get('ville_std'),
                        'competence': info['competence'],
                        'famille':    info['famille'],
                        'date_pub':   str(offre.get('date_publication', ''))[:10],
                        'annee':      str(offre.get('annee', '')),
                        'mois':       str(offre.get('mois', '')),
                    })

        if not competences_trouvees:
            nb_sans_competence += 1
            resultats.append({
                'id_offre':   offre['id_offre'],
                'profil':     offre.get('profil_normalise'),
                'ville':      offre.get('ville_std'),
                'competence': 'non_détecté',
                'famille':    'inconnu',
                'date_pub':   str(offre.get('date_publication', ''))[:10],
                'annee':      str(offre.get('annee', '')),
                'mois':       str(offre.get('mois', '')),
            })

    df_competences = pd.DataFrame(resultats)

    nb_offres_avec = df_competences[
        df_competences['competence'] != 'non_détecté'
    ]['id_offre'].nunique()

    print(f"[NLP] {len(df_competences)} lignes compétences extraites")
    print(f"[NLP] {nb_offres_avec}/{len(df)} offres ont au moins 1 compétence détectée")
    print(f"[NLP] {nb_sans_competence} offres sans compétence détectée")

    return df_competences


def sauvegarder_competences(df_competences: pd.DataFrame, data_lake_root: str):
    silver_path = Path(data_lake_root) / 'silver' / 'competences_extraites'
    silver_path.mkdir(parents=True, exist_ok=True)
    chemin = silver_path / 'competences.parquet'
    df_competences.to_parquet(chemin, index=False, compression='snappy')
    taille = chemin.stat().st_size // 1024
    print(f"[NLP] competences.parquet sauvegardé ({taille} Ko)")
    return chemin


if __name__ == "__main__":
    BASE_DIR       = Path(__file__).resolve().parent.parent
    DATA_LAKE_ROOT = BASE_DIR / "data_lake_mexora_rh"
    REFERENTIEL    = BASE_DIR / "data" / "referentiel_competences_it.json"
    SILVER_OFFRES  = DATA_LAKE_ROOT / "silver" / "offres_clean" / "offres_clean.parquet"

    print("=" * 50)
    print("SILVER NLP — Extraction Compétences")
    print("=" * 50)

    print(f"[NLP] Chargement de {SILVER_OFFRES}...")
    df = pd.read_parquet(SILVER_OFFRES)
    print(f"[NLP] {len(df)} offres chargées depuis Silver")

    df_competences = extraire_competences(df, str(REFERENTIEL))
    sauvegarder_competences(df_competences, str(DATA_LAKE_ROOT))

    print("\n[NLP] Top 10 compétences détectées :")
    top = (
        df_competences[df_competences['competence'] != 'non_détecté']
        .groupby('competence')['id_offre']
        .nunique()
        .sort_values(ascending=False)
        .head(10)
    )
    for comp, nb in top.items():
        print(f"      {comp:20s} : {nb} offres")

    print("\nOK Silver NLP terminé !")