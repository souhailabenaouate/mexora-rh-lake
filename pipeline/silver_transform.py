import json
import re
import pandas as pd
from pathlib import Path

def charger_depuis_bronze(data_lake_root):
    print("[SILVER] Chargement des données depuis Bronze...")
    all_offres = []
    bronze_path = Path(data_lake_root) / 'bronze'
    for json_file in bronze_path.rglob('offres_raw.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        all_offres.extend(data.get('offres', []))
    df = pd.DataFrame(all_offres)
    print(f"[SILVER] {len(df)} offres chargées depuis Bronze")
    return df

def normaliser_villes(df):
    print("[SILVER] Normalisation des villes...")
    mapping_villes = {
        'casablanca': 'Casablanca', 'casa': 'Casablanca',
        'CASABLANCA': 'Casablanca', 'casablnca': 'Casablanca',
        'rabat': 'Rabat', 'RABAT': 'Rabat', 'rabat-salé': 'Rabat',
        'tanger': 'Tanger', 'TANGER': 'Tanger', 'tangier': 'Tanger',
        'tanger-tétouan': 'Tanger',
        'marrakech': 'Marrakech', 'MARRAKECH': 'Marrakech', 'marrakesh': 'Marrakech',
        'fès': 'Fès', 'fes': 'Fès', 'FES': 'Fès',
        'agadir': 'Agadir', 'AGADIR': 'Agadir',
        'kenitra': 'Kénitra', 'kénitra': 'Kénitra',
        'meknes': 'Meknès', 'meknès': 'Meknès',
        'oujda': 'Oujda',
        'remote': 'Remote', 'télétravail': 'Remote', 'full remote': 'Remote',
    }
    def normaliser_ville(ville):
        if pd.isna(ville):
            return 'Non précisé'
        v = str(ville).strip()
        return mapping_villes.get(v, mapping_villes.get(v.lower(), v))
    df['ville_std'] = df['ville'].apply(normaliser_ville)
    df['region_admin'] = df['ville_std'].map({
        'Casablanca': 'Casablanca-Settat',
        'Rabat': 'Rabat-Salé-Kénitra',
        'Tanger': 'Tanger-Tétouan-Al Hoceïma',
        'Marrakech': 'Marrakech-Safi',
        'Fès': 'Fès-Meknès',
        'Agadir': 'Souss-Massa',
        'Kénitra': 'Rabat-Salé-Kénitra',
        'Meknès': 'Fès-Meknès',
        'Oujda': 'Oriental',
        'Remote': 'Remote',
    }).fillna('Autre')
    print(f"[SILVER] Villes normalisées : {df['ville_std'].nunique()} villes uniques")
    return df

def normaliser_titres(df):
    print("[SILVER] Normalisation des titres de poste...")
    mapping_profils = {
        r'data\s*eng|ingénieur\s+data|dev\s+data|etl\s*dev|pipeline\s*dev': 'Data Engineer',
        r'data\s*anal|analyste?\s+data|bi\s+anal|business\s+intel|ingénieur\s+bi|développeur\s+bi|reporting': 'Data Analyst',
        r'data\s*sci|machine\s*learn|ml\s*eng|ia\s*eng|deep\s*learn|nlp': 'Data Scientist',
        r'full\s*stack|fullstack': 'Développeur Full Stack',
        r'back[\s-]*end|backend': 'Développeur Backend',
        r'front[\s-]*end|frontend': 'Développeur Frontend',
        r'mobile|ios\s+dev|android': 'Développeur Mobile',
        r'devops|sre|site\s*reliab': 'DevOps / SRE',
        r'cloud\s*(arch|eng|admin)|aws\s+eng|azure\s+eng': 'Cloud Engineer',
        r'cyber|sécurité\s+info|pentester|soc\s+anal': 'Cybersécurité',
        r'chef\s+de\s+proj|project\s+man|scrum': 'Chef de Projet IT',
        r'architect': 'Architecte IT',
    }
    df['profil_normalise'] = 'Autre IT'
    for pattern, profil in mapping_profils.items():
        masque = df['titre_poste'].str.lower().str.contains(pattern, regex=True, na=False)
        df.loc[masque, 'profil_normalise'] = profil
    print(f"[SILVER] Profils : {df['profil_normalise'].value_counts().to_dict()}")
    return df

def normaliser_contrats(df):
    print("[SILVER] Normalisation des contrats...")
    def normaliser_contrat(val):
        if pd.isna(val):
            return 'Non précisé'
        v = str(val).lower()
        if any(x in v for x in ['cdi', 'indéterminée', 'permanent', 'full-time']):
            return 'CDI'
        if any(x in v for x in ['cdd', 'déterminée', 'fixed']):
            return 'CDD'
        if any(x in v for x in ['freelance', 'indépendant', 'mission', 'consultant']):
            return 'Freelance'
        if any(x in v for x in ['stage', 'internship', 'pfe', 'pfa']):
            return 'Stage'
        return val
    df['type_contrat_std'] = df['type_contrat'].apply(normaliser_contrat)
    return df

def normaliser_salaires(df):
    print("[SILVER] Normalisation des salaires...")
    TAUX_EUR_MAD = 10.8
    def parser_salaire(valeur):
        if pd.isna(valeur) or str(valeur).lower() in ['null', 'confidentiel', 'selon profil', '']:
            return None, None, False
        s = str(valeur).lower().replace(' ', '').replace('\u202f', '')
        est_eur = 'eur' in s or '€' in s
        s = s.replace('eur', '').replace('€', '').replace('mad', '').replace('dh', '')
        s = re.sub(r'(\d+(?:\.\d+)?)k', lambda m: str(int(float(m.group(1)) * 1000)), s)
        nombres = re.findall(r'\d+(?:\.\d+)?', s)
        if not nombres:
            return None, None, False
        montants = [float(n) for n in nombres]
        if est_eur:
            montants = [m * TAUX_EUR_MAD for m in montants]
        if len(montants) >= 2:
            sal_min = min(montants[:2])
            sal_max = max(montants[:2])
        else:
            sal_min = sal_max = montants[0]
        if sal_min < 3000 or sal_max > 100000:
            return None, None, False
        return sal_min, sal_max, True
    resultats = df['salaire_brut'].apply(
        lambda x: pd.Series(parser_salaire(x), index=['salaire_min_mad', 'salaire_max_mad', 'salaire_connu'])
    )
    df = pd.concat([df, resultats], axis=1)
    df['salaire_median_mad'] = (df['salaire_min_mad'] + df['salaire_max_mad']) / 2
    pct = df['salaire_connu'].mean() * 100
    print(f"[SILVER] {pct:.1f}% des offres ont un salaire valide")
    return df

def normaliser_experience(df):
    print("[SILVER] Normalisation de l'expérience...")
    def parser_experience(valeur):
        if pd.isna(valeur):
            return None, None
        s = str(valeur).lower()
        if any(mot in s for mot in ['débutant', 'junior', 'stage', 'sans expérience', '0-1']):
            return 0, 2
        if any(mot in s for mot in ['senior', 'confirmé', 'expert', 'lead', '7+']):
            return 5, None
        fourchette = re.search(r'(\d+)\s*[-àa]\s*(\d+)', s)
        if fourchette:
            return int(fourchette.group(1)), int(fourchette.group(2))
        min_seul = re.search(r'(\d+)\s*(?:ans?|years?)', s)
        if min_seul:
            return int(min_seul.group(1)), None
        return None, None
    resultats = df['experience_requise'].apply(
        lambda x: pd.Series(parser_experience(x), index=['experience_min_ans', 'experience_max_ans'])
    )
    df = pd.concat([df, resultats], axis=1)
    return df

def ajouter_dates(df):
    df['date_publication'] = pd.to_datetime(df['date_publication'], errors='coerce')
    df['date_expiration'] = pd.to_datetime(df['date_expiration'], errors='coerce')
    df['annee'] = df['date_publication'].dt.year.astype('Int64').astype(str)
    df['mois'] = df['date_publication'].dt.month.apply(lambda x: f"{x:02d}" if pd.notna(x) else '')
    df['date_coherente'] = df['date_publication'] <= df['date_expiration']
    nb_incoherentes = (~df['date_coherente']).sum()
    print(f"[SILVER] {nb_incoherentes} dates incohérentes détectées (publication > expiration)")
    return df

def sauvegarder_silver(df, data_lake_root):
    silver_path = Path(data_lake_root) / 'silver' / 'offres_clean'
    silver_path.mkdir(parents=True, exist_ok=True)
    chemin = silver_path / 'offres_clean.parquet'
    df.to_parquet(chemin, index=False, compression='snappy')
    taille = chemin.stat().st_size // 1024
    print(f"[SILVER] offres_clean.parquet sauvegardé ({taille} Ko)")
    return chemin

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_LAKE_ROOT = BASE_DIR / "data_lake_mexora_rh"

    print("=" * 50)
    print("SILVER TRANSFORM — Mexora RH Lake")
    print("=" * 50)

    df = charger_depuis_bronze(str(DATA_LAKE_ROOT))
    df = normaliser_villes(df)
    df = normaliser_titres(df)
    df = normaliser_contrats(df)
    df = normaliser_salaires(df)
    df = normaliser_experience(df)
    df = ajouter_dates(df)

    print(f"\n[SILVER] Résumé final : {len(df)} offres nettoyées")
    print(f"[SILVER] Colonnes : {list(df.columns)}")

    sauvegarder_silver(df, str(DATA_LAKE_ROOT))
    print("\nOK Silver Transform terminé !")