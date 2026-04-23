"""
generate_data.py
Génère les 3 fichiers de données du projet Mexora RH Lake :
  1. offres_emploi_it_maroc.json      (5 000 offres)
  2. referentiel_competences_it.json  (référentiel compétences)
  3. entreprises_it_maroc.csv         (entreprises IT marocaines)
"""

import json
import csv
import random
import re
from datetime import datetime, timedelta

random.seed(42)

# ─────────────────────────────────────────────
# DONNÉES DE RÉFÉRENCE
# ─────────────────────────────────────────────

VILLES_VARIANTS = {
    "Casablanca": ["Casablanca", "casablanca", "CASABLANCA", "Casa", "casa", "Casablnca"],
    "Rabat":      ["Rabat", "rabat", "RABAT", "Rabat-Salé"],
    "Tanger":     ["Tanger", "tanger", "TANGER", "Tanger-Tétouan", "Tangier"],
    "Marrakech":  ["Marrakech", "marrakech", "MARRAKECH", "Marrakesh"],
    "Fès":        ["Fès", "Fes", "FES", "fès"],
    "Agadir":     ["Agadir", "agadir", "AGADIR"],
    "Kénitra":    ["Kénitra", "Kenitra", "kenitra"],
    "Meknès":     ["Meknès", "Meknes"],
    "Oujda":      ["Oujda", "oujda"],
    "Remote":     ["Remote", "Télétravail", "Full Remote"],
}

VILLES_POIDS = [35, 20, 12, 8, 5, 5, 4, 3, 3, 5]

SOURCES = ["rekrute", "marocannonce", "linkedin"]
SOURCES_POIDS = [50, 25, 25]

POSTES_PAR_PROFIL = {
    "Data Engineer": [
        "Data Engineer", "Ingénieur Data", "Data Eng.", "Dev Data",
        "Ingénieur Big Data", "Data Engineer Junior", "Data Engineer Senior",
        "Ingénieur ETL", "Pipeline Developer", "Data Infrastructure Engineer",
    ],
    "Data Analyst": [
        "Data Analyst", "Analyste Data", "Analyste BI", "Business Intelligence Analyst",
        "Développeur BI", "Ingénieur BI", "Reporting Analyst", "BI Analyst",
        "Analyste de Données", "Data Analytics Engineer",
    ],
    "Data Scientist": [
        "Data Scientist", "Machine Learning Engineer", "ML Engineer",
        "Ingénieur Machine Learning", "IA Engineer", "Deep Learning Engineer",
        "NLP Engineer", "AI Scientist", "Data Science Engineer",
    ],
    "Développeur Full Stack": [
        "Développeur Full Stack", "Full Stack Developer", "Fullstack Developer",
        "Dev Full Stack", "Full Stack Engineer", "Développeur Web Full Stack",
    ],
    "Développeur Backend": [
        "Développeur Backend", "Backend Developer", "Back-End Developer",
        "Ingénieur Backend", "Dev Backend", "Software Engineer Backend",
    ],
    "Développeur Frontend": [
        "Développeur Frontend", "Frontend Developer", "Front-End Developer",
        "Dev Frontend", "UI Developer", "Développeur React",
    ],
    "DevOps / SRE": [
        "DevOps Engineer", "SRE Engineer", "Ingénieur DevOps",
        "DevOps Developer", "Cloud DevOps", "Site Reliability Engineer",
    ],
    "Cloud Engineer": [
        "Cloud Engineer", "Ingénieur Cloud", "AWS Engineer",
        "Azure Engineer", "GCP Engineer", "Cloud Architect",
    ],
    "Cybersécurité": [
        "Cybersecurity Engineer", "Ingénieur Cybersécurité",
        "SOC Analyst", "Pentester", "Security Engineer",
    ],
    "Chef de Projet IT": [
        "Chef de Projet IT", "Project Manager IT", "Scrum Master",
        "Agile Coach", "IT Project Manager",
    ],
}

COMPETENCES_PAR_PROFIL = {
    "Data Engineer":          ["python", "spark", "airflow", "kafka", "sql", "dbt", "hadoop", "aws", "azure", "docker", "git"],
    "Data Analyst":           ["sql", "python", "power bi", "tableau", "excel", "metabase", "looker", "r", "matplotlib"],
    "Data Scientist":         ["python", "r", "tensorflow", "pytorch", "scikit-learn", "sql", "spark", "aws", "docker", "git"],
    "Développeur Full Stack": ["javascript", "react", "node.js", "python", "sql", "docker", "git", "angular", "mongodb"],
    "Développeur Backend":    ["java", "python", "spring", "sql", "docker", "git", "postgresql", "aws", "redis"],
    "Développeur Frontend":   ["javascript", "react", "angular", "css", "html", "git", "typescript", "vue.js"],
    "DevOps / SRE":           ["docker", "kubernetes", "aws", "azure", "git", "jenkins", "terraform", "python", "linux"],
    "Cloud Engineer":         ["aws", "azure", "gcp", "docker", "kubernetes", "terraform", "python", "git"],
    "Cybersécurité":          ["python", "linux", "aws", "firewalls", "siem", "git", "penetration testing"],
    "Chef de Projet IT":      ["agile", "scrum", "jira", "ms project", "python", "sql", "power bi"],
}

CONTRATS_VARIANTS = {
    "CDI": ["CDI", "cdi", "Contrat à durée indéterminée", "Permanent", "Full-time CDI"],
    "CDD": ["CDD", "cdd", "Contrat à durée déterminée", "Fixed Term"],
    "Freelance": ["Freelance", "freelance", "Indépendant", "Mission freelance", "Consultant"],
    "Stage": ["Stage", "stage", "Internship", "Stage PFE", "Stage PFA"],
}
CONTRATS_POIDS = [55, 15, 20, 10]

EXPERIENCE_VARIANTS = [
    "Débutant accepté", "0-1 ans", "1-2 ans", "2-3 ans",
    "3 à 5 ans", "3-5 ans", "min 3 ans", "5-7 ans",
    "Senior (7+ ans)", "7 à 10 ans", None,
]

NIVEAUX_ETUDES = ["Bac+2", "Bac+3", "Bac+5", "Bac+5 / Master", "Doctorat", "Bac+4"]

SALAIRES_VARIANTS = {
    "Data Engineer":          ("10000-15000 MAD", "15K-20K", "12000-18000 MAD", "Selon profil", None, "15000-22000 MAD"),
    "Data Analyst":           ("8000-12000 MAD", "10K-15K", "Confidentiel", "9000-13000 MAD", None),
    "Data Scientist":         ("15000-22000 MAD", "18K-25K", "Selon profil", "20000-30000 MAD", None),
    "Développeur Full Stack": ("8000-12000 MAD", "10K-15K", "12000-18000 MAD", None, "1200-1600 EUR"),
    "Développeur Backend":    ("9000-14000 MAD", "10K-15K", "Confidentiel", "11000-16000 MAD"),
    "Développeur Frontend":   ("7000-11000 MAD", "8K-12K", None, "9000-13000 MAD"),
    "DevOps / SRE":           ("12000-18000 MAD", "15K-22K", "Selon profil", "14000-20000 MAD"),
    "Cloud Engineer":         ("13000-20000 MAD", "15K-22K", "Confidentiel", "16000-24000 MAD"),
    "Cybersécurité":          ("12000-18000 MAD", "14K-20K", None, "Selon profil"),
    "Chef de Projet IT":      ("10000-16000 MAD", "12K-18K", "Confidentiel", None),
}

TELETRAVAIL_OPTIONS = ["Présentiel", "Hybride", "Full Remote", "Télétravail partiel"]
TELETRAVAIL_POIDS  = [40, 35, 15, 10]

SECTEURS = [
    "Informatique / Télécom", "Finance / Banque", "E-commerce",
    "Conseil IT", "Industrie", "Santé", "Éducation", "Logistique",
]

ENTREPRISES = [
    "TechMaroc SARL", "DataSolutions MA", "InnovateTech Maroc",
    "CasaTech Group", "MarocDigital", "AtlasData", "NumériqueMA",
    "InfoSys Maroc", "TangerTech", "RabatInnovation",
    "MegaSoft Maroc", "CloudFirst MA", "BigDataMaroc",
    "AnalyticsPro", "DevHouse Maroc", "TechVenture Casa",
    "Insight Analytics", "DataBridge MA", "NextGenTech",
    "MarocSoft", "BI Consulting MA", "Hexatech",
    "Axionable Maroc", "Sofrecom", "Capgemini Maroc",
    "IBM Maroc", "Orange Business Services", "Inwi",
    "Maroc Telecom", "CIH Bank", "Attijariwafa Bank",
    "BMCE Group", "OCP Group", "Manpower Maroc",
]

LANGUES = [
    ["Français"],
    ["Français", "Anglais"],
    ["Arabe", "Français"],
    ["Arabe", "Français", "Anglais"],
    ["Anglais"],
]

DESCRIPTIONS_TEMPLATES = [
    "Nous recherchons un {poste} expérimenté maîtrisant {comp1}, {comp2} et {comp3}. "
    "Connaissance de {comp4} appréciée. Le candidat devra travailler en méthode Agile "
    "avec une équipe dynamique. Expérience en {comp5} est un plus.",

    "Dans le cadre de notre croissance, nous recrutons un {poste} pour renforcer notre équipe data. "
    "Compétences requises : {comp1}, {comp2}, {comp3}. "
    "Bonus : maîtrise de {comp4} et {comp5}. Ambiance startup, projets innovants.",

    "Rejoignez notre équipe en tant que {poste}. "
    "Vous aurez à travailler avec {comp1}, {comp2}, {comp3}. "
    "Une expérience avec {comp4} est indispensable. {comp5} est un plus significatif.",

    "Poste : {poste}. Stack technique : {comp1}, {comp2}, {comp3}, {comp4}. "
    "Vous interviendrez sur des projets à fort impact. "
    "Maîtrise de {comp5} requise pour ce rôle.",

    "Opportunité pour un {poste} souhaitant évoluer dans un environnement stimulant. "
    "Technologies : {comp1}, {comp2}, {comp3}. "
    "La connaissance de {comp4} et {comp5} sera fortement appréciée.",
]

# ─────────────────────────────────────────────
# GÉNÉRATION DES OFFRES
# ─────────────────────────────────────────────

def generer_date(start="2023-01-01", end="2024-11-30"):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end,   "%Y-%m-%d")
    delta    = (end_dt - start_dt).days
    return start_dt + timedelta(days=random.randint(0, delta))

def generer_offre(index: int) -> dict:
    # Profil et poste
    profil = random.choices(list(POSTES_PAR_PROFIL.keys()),
                             weights=[15, 12, 10, 18, 10, 8, 8, 7, 6, 6])[0]
    titre  = random.choice(POSTES_PAR_PROFIL[profil])

    # Source et ville
    source = random.choices(SOURCES, weights=SOURCES_POIDS)[0]
    ville_std = random.choices(list(VILLES_VARIANTS.keys()), weights=VILLES_POIDS)[0]
    ville = random.choice(VILLES_VARIANTS[ville_std])

    # Dates
    date_pub = generer_date()
    # Intentional bug : ~3% of offers have expiration before publication
    if random.random() < 0.03:
        date_exp = date_pub - timedelta(days=random.randint(1, 10))
    else:
        date_exp = date_pub + timedelta(days=random.randint(20, 60))

    # Compétences
    comps_pool = COMPETENCES_PAR_PROFIL[profil]
    nb_comps   = random.randint(3, min(7, len(comps_pool)))
    comps      = random.sample(comps_pool, nb_comps)

    # Séparateurs incohérents intentionnels
    sep = random.choice([", ", " / ", " • ", "\n", " | "])
    competences_brut = sep.join(comps)

    # Description
    tpl = random.choice(DESCRIPTIONS_TEMPLATES)
    comps_padded = comps + comps  # ensure enough elements
    description = tpl.format(
        poste=titre,
        comp1=comps_padded[0], comp2=comps_padded[1],
        comp3=comps_padded[2], comp4=comps_padded[3],
        comp5=comps_padded[4],
    )

    # Salaire
    sal_options = SALAIRES_VARIANTS.get(profil, ("Selon profil", None))
    salaire_brut = random.choice(sal_options)

    # Contrat
    contrat_std = random.choices(list(CONTRATS_VARIANTS.keys()), weights=CONTRATS_POIDS)[0]
    contrat     = random.choice(CONTRATS_VARIANTS[contrat_std])

    # Expérience
    experience = random.choice(EXPERIENCE_VARIANTS)

    # ID
    source_prefix = {"rekrute": "RK", "marocannonce": "MA", "linkedin": "LI"}[source]
    id_offre = f"{source_prefix}-{date_pub.year}-{str(index).zfill(5)}"

    return {
        "id_offre":          id_offre,
        "source":            source,
        "titre_poste":       titre,
        "description":       description,
        "competences_brut":  competences_brut,
        "entreprise":        random.choice(ENTREPRISES),
        "ville":             ville,
        "type_contrat":      contrat,
        "experience_requise": experience,
        "salaire_brut":      salaire_brut,
        "niveau_etudes":     random.choice(NIVEAUX_ETUDES),
        "secteur":           random.choice(SECTEURS),
        "date_publication":  date_pub.strftime("%Y-%m-%d"),
        "date_expiration":   date_exp.strftime("%Y-%m-%d"),
        "nb_postes":         random.choice([1, 1, 1, 2, 3]),
        "teletravail":       random.choices(TELETRAVAIL_OPTIONS, weights=TELETRAVAIL_POIDS)[0],
        "langue_requise":    random.choice(LANGUES),
    }

def generer_offres_json(filepath: str, nb: int = 5000):
    print(f"[GEN] Génération de {nb} offres...")
    offres = [generer_offre(i + 1) for i in range(nb)]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"offres": offres}, f, ensure_ascii=False, indent=2)
    print(f"[GEN] ✅ {filepath} créé ({len(offres)} offres)")

# ─────────────────────────────────────────────
# RÉFÉRENTIEL COMPÉTENCES
# ─────────────────────────────────────────────

REFERENTIEL = {
    "familles": {
        "langages": {
            "python":     ["python", "python3", "py"],
            "javascript": ["javascript", "js", "node.js", "nodejs", "node"],
            "java":       ["java", "java8", "java11", "java17"],
            "sql":        ["sql", "mysql", "postgresql", "postgres", "oracle", "tsql"],
            "r":          ["r", "rlang", "r-studio"],
            "typescript": ["typescript", "ts"],
            "scala":      ["scala"],
            "go":         ["go", "golang"],
            "c_sharp":    ["c#", "csharp", ".net"],
            "php":        ["php", "laravel", "symfony"],
        },
        "frameworks_web": {
            "react":   ["react", "reactjs", "react.js"],
            "angular": ["angular", "angularjs"],
            "vue":     ["vue.js", "vuejs", "vue"],
            "django":  ["django", "django rest"],
            "spring":  ["spring", "spring boot", "springboot"],
            "fastapi": ["fastapi", "fast api"],
            "flask":   ["flask"],
        },
        "data_engineering": {
            "spark":   ["spark", "apache spark", "pyspark"],
            "kafka":   ["kafka", "apache kafka"],
            "airflow": ["airflow", "apache airflow"],
            "dbt":     ["dbt", "data build tool"],
            "hadoop":  ["hadoop", "hdfs", "mapreduce"],
        },
        "cloud": {
            "aws":   ["aws", "amazon web services", "ec2", "s3", "lambda"],
            "gcp":   ["gcp", "google cloud", "bigquery", "cloud storage"],
            "azure": ["azure", "microsoft azure", "synapse"],
        },
        "bi_analytics": {
            "power_bi":  ["power bi", "powerbi", "pbi"],
            "tableau":   ["tableau", "tableau desktop"],
            "metabase":  ["metabase"],
            "looker":    ["looker", "looker studio"],
            "excel":     ["excel", "microsoft excel"],
            "matplotlib":["matplotlib", "seaborn", "plotly"],
        },
        "devops_infra": {
            "docker":     ["docker", "docker-compose", "dockerfile"],
            "kubernetes": ["kubernetes", "k8s", "kubectl"],
            "git":        ["git", "github", "gitlab", "bitbucket"],
            "terraform":  ["terraform", "iac"],
            "jenkins":    ["jenkins", "ci/cd", "cicd"],
            "linux":      ["linux", "ubuntu", "centos"],
        },
        "ml_ia": {
            "scikit_learn": ["scikit-learn", "sklearn"],
            "tensorflow":   ["tensorflow", "tf", "keras"],
            "pytorch":      ["pytorch", "torch"],
        },
        "bases_de_donnees": {
            "mongodb":  ["mongodb", "mongo"],
            "redis":    ["redis"],
            "elasticsearch": ["elasticsearch", "elastic"],
        },
        "methodologies": {
            "agile":   ["agile", "scrum", "kanban"],
            "jira":    ["jira", "confluence"],
        },
    }
}

def generer_referentiel(filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(REFERENTIEL, f, ensure_ascii=False, indent=2)
    print(f"[GEN] ✅ {filepath} créé")

# ─────────────────────────────────────────────
# ENTREPRISES IT MAROC
# ─────────────────────────────────────────────

ENTREPRISES_DATA = [
    ("TechMaroc SARL",       "Informatique",  "PME",              "Casablanca", "techmaroc.ma",    "SSII"),
    ("DataSolutions MA",     "Data / BI",     "PME",              "Casablanca", "datasolutions.ma","Produit"),
    ("InnovateTech Maroc",   "Informatique",  "Startup",          "Rabat",      "innovatetech.ma", "Produit"),
    ("CasaTech Group",       "Télécom",       "Grande Entreprise", "Casablanca", "casatech.ma",    "Telecom"),
    ("MarocDigital",         "E-commerce",    "ETI",              "Casablanca", "marocdigital.ma", "Produit"),
    ("AtlasData",            "Data / BI",     "Startup",          "Rabat",      "atlasdata.ma",    "Conseil"),
    ("NumériqueMA",          "Informatique",  "PME",              "Tanger",     "numeriquema.ma",  "SSII"),
    ("InfoSys Maroc",        "Informatique",  "Grande Entreprise", "Casablanca", "infosys.com",    "SSII"),
    ("TangerTech",           "Informatique",  "PME",              "Tanger",     "tangertech.ma",   "Produit"),
    ("RabatInnovation",      "Informatique",  "Startup",          "Rabat",      "rabatinno.ma",    "Produit"),
    ("MegaSoft Maroc",       "Logiciels",     "ETI",              "Casablanca", "megasoft.ma",     "Produit"),
    ("CloudFirst MA",        "Cloud",         "Startup",          "Casablanca", "cloudfirst.ma",   "Conseil"),
    ("BigDataMaroc",         "Data",          "PME",              "Casablanca", "bigdatamaroc.ma", "Conseil"),
    ("AnalyticsPro",         "Data / BI",     "PME",              "Rabat",      "analyticspro.ma", "Conseil"),
    ("DevHouse Maroc",       "Informatique",  "PME",              "Marrakech",  "devhouse.ma",     "SSII"),
    ("TechVenture Casa",     "Informatique",  "Startup",          "Casablanca", "techventure.ma",  "Produit"),
    ("Insight Analytics",    "Data",          "PME",              "Tanger",     "insight-ma.ma",   "Conseil"),
    ("DataBridge MA",        "Data",          "Startup",          "Casablanca", "databridge.ma",   "Produit"),
    ("NextGenTech",          "Informatique",  "ETI",              "Rabat",      "nextgentech.ma",  "SSII"),
    ("MarocSoft",            "Logiciels",     "PME",              "Fès",        "marocsoft.ma",    "Produit"),
    ("BI Consulting MA",     "BI / Analytics","PME",              "Casablanca", "biconsulting.ma", "Conseil"),
    ("Hexatech",             "Informatique",  "PME",              "Agadir",     "hexatech.ma",     "SSII"),
    ("Axionable Maroc",      "Data / IA",     "ETI",              "Casablanca", "axionable.com",   "Conseil"),
    ("Sofrecom",             "Télécom",       "Grande Entreprise", "Rabat",     "sofrecom.com",    "Conseil"),
    ("Capgemini Maroc",      "Informatique",  "Grande Entreprise", "Casablanca","capgemini.com",   "SSII"),
    ("IBM Maroc",            "Informatique",  "Grande Entreprise", "Casablanca","ibm.com",         "Produit"),
    ("Orange Business",      "Télécom",       "Grande Entreprise", "Casablanca","orange.com",      "Telecom"),
    ("Inwi",                 "Télécom",       "Grande Entreprise", "Rabat",     "inwi.ma",         "Telecom"),
    ("Maroc Telecom",        "Télécom",       "Grande Entreprise", "Rabat",     "iam.ma",          "Telecom"),
    ("CIH Bank",             "Finance",       "Grande Entreprise", "Casablanca","cih.co.ma",       "Banque"),
    ("Attijariwafa Bank",    "Finance",       "Grande Entreprise", "Casablanca","attijariwafabank.com","Banque"),
    ("BMCE Group",           "Finance",       "Grande Entreprise", "Casablanca","bmcebank.ma",     "Banque"),
    ("OCP Group",            "Industrie",     "Grande Entreprise", "Casablanca","ocpgroup.ma",     "Autre"),
    ("Manpower Maroc",       "RH / Recrutement","ETI",            "Casablanca", "manpower.ma",     "Conseil"),
]

def generer_entreprises_csv(filepath: str):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nom_entreprise", "secteur", "taille", "ville_siege", "site_web", "type"])
        for row in ENTREPRISES_DATA:
            writer.writerow(row)
    print(f"[GEN] ✅ {filepath} créé ({len(ENTREPRISES_DATA)} entreprises)")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)

    generer_offres_json("data/offres_emploi_it_maroc.json", nb=5000)
    generer_referentiel("data/referentiel_competences_it.json")
    generer_entreprises_csv("data/entreprises_it_maroc.csv")

    print("\n✅ Tous les fichiers de données ont été générés dans le dossier data/")
