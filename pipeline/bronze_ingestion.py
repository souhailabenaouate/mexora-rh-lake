import json
import os
from datetime import datetime
from pathlib import Path

def ingerer_bronze(filepath_source, data_lake_root):
    print(f"[BRONZE] Chargement : {filepath_source}")
    with open(filepath_source, 'r', encoding='utf-8') as f:
        data = json.load(f)
    offres = data.get('offres', [])
    partitions = {}
    for offre in offres:
        source = offre.get('source', 'inconnu').lower().replace(' ', '_')
        date_pub = offre.get('date_publication', '')
        try:
            mois_partition = datetime.strptime(date_pub[:7], '%Y-%m').strftime('%Y_%m')
        except:
            mois_partition = 'date_inconnue'
        cle = f"{source}/{mois_partition}"
        if cle not in partitions:
            partitions[cle] = []
        partitions[cle].append(offre)
    nb_fichiers = 0
    par_source = {}
    for partition, offres_partition in partitions.items():
        chemin_dir = os.path.join(data_lake_root, 'bronze', partition)
        os.makedirs(chemin_dir, exist_ok=True)
        chemin_fichier = os.path.join(chemin_dir, 'offres_raw.json')
        with open(chemin_fichier, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'date_ingestion': datetime.now().isoformat(),
                    'partition': partition,
                    'nb_offres': len(offres_partition)
                },
                'offres': offres_partition
            }, f, ensure_ascii=False, indent=2)
        nb_fichiers += 1
        source_nom = partition.split('/')[0]
        par_source[source_nom] = par_source.get(source_nom, 0) + len(offres_partition)
    print(f"[BRONZE] OK {len(offres)} offres dans {nb_fichiers} partitions")
    for s, n in par_source.items():
        print(f"         - {s} : {n} offres")
    return {'total': len(offres), 'nb_fichiers': nb_fichiers, 'par_source': par_source}

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent
    FILEPATH_SOURCE = BASE_DIR / "data" / "offres_emploi_it_maroc.json"
    DATA_LAKE_ROOT = BASE_DIR / "data_lake_mexora_rh"
    print("=" * 50)
    print("BRONZE INGESTION — Mexora RH Lake")
    print("=" * 50)
    ingerer_bronze(str(FILEPATH_SOURCE), str(DATA_LAKE_ROOT))
    bronze_path = DATA_LAKE_ROOT / "bronze"
    nb_json = list(bronze_path.rglob("offres_raw.json"))
    print(f"\n{len(nb_json)} fichiers crees dans Bronze")
    print("OK Ingestion Bronze terminee !")