import json
import pandas as pd
from db_connection import fetch_all_sources
from chaine_markov import ChaineMarkov

def charger_mapping_officiel(chemin_mapping="mapping_etats.json"):
    try:
        with open(chemin_mapping, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erreur mapping : {e}")
        return {}

def lancer_audit():
    with open('db_config.json') as f:
        config = json.load(f)
    
    mapping_config = charger_mapping_officiel()

    # 1. Extraction SQL
    pattern = 'ap1_b21718309.dat'
    df_sql = fetch_all_sources(config, pattern)

    if df_sql.empty:
        print("❌ Aucune donnée.")
        return

    # 2. Application du Mapping
    def mapper(val):
        for etat, liste in mapping_config.items():
            if val in liste: return etat
        return "S_Leak"

    df_reel = df_sql.copy()
    
    # IMPORTANT : On définit bien qui est la SOURCE (Xn) et qui est la DESTINATION (Xn+1)
    df_reel['etat_source'] = df_reel['dest_collect'].apply(mapper)
    df_reel['etat_arrivee'] = df_reel['dest_proc'].apply(mapper)

    # 3. Calcul Markovien par état
    etats = list(mapping_config.keys())
    markov = ChaineMarkov(idMarkov="Audit_Sonatel", Etats=etats)
    
    # Le group by se fera à l'intérieur sur (etat_source, etat_arrivee)
    resultats = markov.calculerProbabilites(df_reel)

    print("\n--- RÉSULTATS (Probabilités Conditionnelles P(Xn+1|Xn)) ---")
    print(json.dumps(resultats, indent=4))

if __name__ == "__main__":
    lancer_audit()