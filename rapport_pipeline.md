# Rapport Pipeline — Mexora RH Lake

## Transformations Bronze → Silver → Gold

---

## 1. Bronze Ingestion

**Règle appliquée :** Chargement des données brutes sans aucune modification. Partitionnement par source (rekrute, linkedin, marocannonce) et par mois de publication.

| Métrique | Valeur |
|----------|--------|
| Offres chargées | 5 000 |
| Partitions créées | 69 |
| Sources | rekrute (2 496), linkedin (1 257), marocannonce (1 247) |
| Période couverte | Janvier 2023 — Novembre 2024 |

**Principe fondamental :** La zone Bronze est immuable. Aucune donnée n'est modifiée après ingestion. C'est l'archive fidèle de la source.

---

## 2. Silver Transform — Nettoyage

### 2.1 Normalisation des villes

**Règle :** Mapping des variantes orthographiques vers une forme canonique.

| Avant | Après |
|-------|-------|
| casa, CASABLANCA, casablnca | Casablanca |
| tanger, TANGER, Tangier | Tanger |
| rabat-salé | Rabat |
| Full Remote, Télétravail | Remote |

| Métrique | Valeur |
|----------|--------|
| Avant | Nombreuses variantes incohérentes |
| Après | 10 villes standardisées |

### 2.2 Normalisation des titres de poste

**Règle :** Regex matching vers 12 profils IT normalisés.

| Profil | Nombre d'offres |
|--------|----------------|
| Développeur Full Stack | 918 |
| Autre IT | 659 |
| Data Engineer | 538 |
| Développeur Backend | 527 |
| Data Analyst | 527 |
| Data Scientist | 430 |
| DevOps / SRE | 423 |
| Cybersécurité | 259 |
| Développeur Frontend | 258 |
| Chef de Projet IT | 230 |
| Cloud Engineer | 182 |
| Architecte IT | 49 |

### 2.3 Normalisation des contrats

**Règle :** Mapping vers 4 types standardisés (CDI, CDD, Freelance, Stage).

Exemples traités :
- "Contrat à durée indéterminée", "Permanent", "cdi" → CDI
- "Mission freelance", "Indépendant" → Freelance
- "Stage PFE", "Internship" → Stage

### 2.4 Normalisation des salaires

**Règle :** Extraction des montants min/max en MAD mensuel brut.

| Cas | Traitement |
|-----|-----------|
| "15K-20K" | Multiplié par 1000 → 15000-20000 MAD |
| "1200-1600 EUR" | Converti au taux 1 EUR = 10.8 MAD |
| "Selon profil", null, "Confidentiel" | salaire_connu = False |
| Montant < 3000 ou > 100000 MAD | Rejeté comme incohérent |

| Métrique | Valeur |
|----------|--------|
| Offres avec salaire valide | 68.9% (3 445 offres) |
| Offres sans salaire | 31.1% (1 555 offres) |

### 2.5 Normalisation de l'expérience

**Règle :** Extraction des années min/max depuis texte libre.

| Entrée | experience_min | experience_max |
|--------|---------------|---------------|
| "Débutant accepté" | 0 | 2 |
| "3-5 ans" | 3 | 5 |
| "min 3 ans" | 3 | None |
| "Senior (7+ ans)" | 5 | None |

### 2.6 Cohérence des dates

**Règle :** Détection des offres où date_publication > date_expiration.

| Métrique | Valeur |
|----------|--------|
| Dates incohérentes détectées | 145 (2.9% des offres) |
| Traitement | Flagué dans colonne date_coherente = False |

---

## 3. Silver NLP — Extraction des compétences

**Règle :** Matching par regex (word boundary) sur le référentiel de 300 compétences IT normalisées. Sources : champ competences_brut + champ description.

**Stratégie :** Les aliases sont triés par longueur décroissante pour éviter les faux positifs (ex: "node" ne matche pas avant "node.js").

| Métrique | Valeur |
|----------|--------|
| Lignes compétences extraites | ~25 000+ |
| Offres avec ≥1 compétence | ~4 900/5 000 |
| Familles de compétences | 9 familles |

---

## 4. Gold Aggregation

5 tables analytiques construites via DuckDB :

| Table | Description | Lignes |
|-------|-------------|--------|
| top_competences.parquet | Compétences par profil avec rang | ~500 |
| salaires_par_profil.parquet | Stats salariales par profil/ville | ~200 |
| offres_par_ville.parquet | Volume offres par ville/profil/mois | ~800 |
| entreprises_recruteurs.parquet | Top 100 entreprises recruteurs | ~100 |
| tendances_mensuelles.parquet | Évolution mensuelle par profil | ~300 |

---

## 5. Cas limites et choix de conception

1. **Données manquantes** : Les champs null sont conservés et flagués plutôt que supprimés pour maintenir le volume d'analyse.
2. **Titres non classifiés** : Les 659 offres "Autre IT" sont conservées car elles représentent des métiers réels non couverts par le mapping.
3. **Salaires en EUR** : Convertis au taux fixe 2024 (1 EUR = 10.8 MAD) avec note dans le rapport.
4. **Dates incohérentes** : Conservées avec flag plutôt que supprimées (2.9% des données).
5. **Bronze immuable** : Jamais modifié après ingestion — toute correction se fait en Silver.