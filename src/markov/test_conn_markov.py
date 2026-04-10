import argparse
import json
from datetime import datetime

from chaine_markov import ChaineMarkov
from connexion_bd import fetch_recent_traces, get_db_conn

WINDOW_MOBILE = (
    datetime.fromisoformat("2026-02-27T08:45:56.024"),
    datetime.fromisoformat("2026-03-04T09:11:14.998"),
)
WINDOW_GGSN = (
    datetime.fromisoformat("2026-03-11T18:32:02.959"),
    datetime.fromisoformat("2026-03-12T16:41:31.450"),
)

_FLUX_NORM = {"msc": "mobile", "data": "ggsn"}


def _parse_datetime(value: str):
    if not value:
        return None
    return datetime.fromisoformat(value)


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Test connexion MySQL locale + calcul probabilites Markov"
    )
    parser.add_argument("--db-config",      default="db_config.json")
    parser.add_argument("--mapping-config", default="config_mapping.json")
    parser.add_argument("--db-name",        default="audit_markov_local")
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--start-time",     default="")
    parser.add_argument("--end-time",       default="")
    parser.add_argument(
        "--flux",
        choices=["mobile", "msc", "ggsn", "data"],
        default="ggsn",
        help="Type de flux : mobile/msc (E1_FILTER) ou ggsn/data (Filter)",
    )
    args = parser.parse_args()

    flux_norm = _FLUX_NORM.get(args.flux.lower(), args.flux.lower())
    is_ggsn   = (flux_norm == "ggsn")
    start_time = _parse_datetime(args.start_time)
    end_time   = _parse_datetime(args.end_time)

    if is_ggsn:
        source_filter    = "Filter"
        reject_from_proc = True
        default_window   = WINDOW_GGSN
    else:
        source_filter    = "E1_FILTER"
        reject_from_proc = False
        default_window   = WINDOW_MOBILE

    db_config = _load_json(args.db_config)
    db_config["databases"] = [
        db for db in db_config.get("databases", [])
        if db.get("name") == args.db_name
    ]
    if not db_config["databases"]:
        print(f"ERREUR: base '{args.db_name}' introuvable dans {args.db_config}")
        return

    # Test connexion MySQL
    print(f"[1/3] Test connexion MySQL : {args.db_name}  |  flux={args.flux.upper()}")
    try:
        conn  = get_db_conn(db_name=args.db_name, config_path=args.db_config)
        table = "audit_markov_event_proc_ggsn" if is_ggsn else "audit_markov_event_proc"
        cur   = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"     CONNEXION OK — '{table}' : {count:,} lignes\n")
    except Exception as exc:
        print(f"     CONNEXION ECHEC : {exc}")
        return

    windows = [(start_time, end_time)] if (start_time and end_time) else [default_window]

    for idx, (cur_start, cur_end) in enumerate(windows, start=1):
        duration = (cur_end - cur_start).total_seconds()
        print(f"{'='*60}")
        print(f"[2/3] Fenetre {idx} : {cur_start} --> {cur_end}  ({duration:.1f}s)")
        print(f"      source_filter={source_filter}  reject_from_proc={reject_from_proc}")
        print(f"{'='*60}")

        df_collect, df_markov_vols, df_agents = fetch_recent_traces(
            db_config=db_config,
            mapping_path=args.mapping_config,
            window_minutes=args.window_minutes,
            start_time=cur_start,
            end_time=cur_end,
            source_filter=source_filter,
            reject_from_proc=reject_from_proc,
        )

        print(f"     Lignes collect     : {len(df_collect)}")
        print(f"     Lignes markov_vols : {len(df_markov_vols)}")
        print(f"     Lignes agents      : {len(df_agents)}")

        if not df_collect.empty:
            print("\n     Apercu collect (5 premieres lignes) :")
            print(df_collect.head().to_string(index=False))

        if df_markov_vols.empty:
            print("     Aucune donnee Markov sur cette fenetre.\n")
            continue

        print(f"\n     Apercu markov_vols :")
        print(df_markov_vols.to_string(index=False))

        if not df_agents.empty:
            print(f"\n     Apercu df_agents (Diagnostics V2 / Fuite Interne) :")
            print(df_agents.to_string(index=False))

        print(f"\n[3/3] Calcul probabilites - fenetre {idx}")
        chaine = ChaineMarkov(idMarkov="Audit_Sonatel")
        stats  = chaine.calculerProbabilites(df_markov_vols, flux=args.flux)
        print(f"     Nombre de stats : {len(stats)}\n")

        if is_ggsn:
            agregats         = [s for s in stats if s.get("is_aggregate")]
            splits           = [s for s in stats if not s.get("is_aggregate")]
            items_a_afficher = agregats + splits
        else:
            items_a_afficher = stats

        for item in items_a_afficher:
            somme  = item["kpi_succes"] + item["kpi_rejet"] + item["kpi_fuite"]
            statut = "OK" if abs(somme - 1.0) < 0.01 else "ATTENTION"
            prefix = f"  {'[AGREGE]' if item.get('is_aggregate') else '[SPLIT] '} " \
                     if is_ggsn else "  "

            print(f"{prefix}workflow_name : {item['workflow_name']}")
            print(f"{prefix}filename      : {item['filename']}")
            print(f"{prefix}n_proc        : {item['n_proc']}")
            print(f"{prefix}n_ok          : {item['n_ok']}")
            print(f"{prefix}n_reject      : {item['n_reject']}")
            print(f"{prefix}n_leak        : {item['n_leak']}")

            if item['n_leak'] > 0 and not df_agents.empty:
                if item.get("is_aggregate"):
                    agents_for_file = df_agents[df_agents["filename"].str.endswith(item['filename'])]
                else:
                    agents_for_file = df_agents[df_agents["filename"] == item['filename']]
                    
                def get_out(a_name):
                    m = agents_for_file[agents_for_file["agent_name"] == a_name]
                    return m["OUT"].sum() if not m.empty else 0
                    
                out_filter = get_out("Filter")
                out_aggr   = get_out("Routing aggregation")
                out_pgw    = get_out("RoutingPGW")
                
                leak_from, leak_to = "N/A", "N/A"
                val_from, val_to   = 0, 0
                if out_filter > out_aggr:
                    leak_from, leak_to = "Filter", "Routing aggregation"
                    val_from, val_to   = out_filter, out_aggr
                elif out_aggr > out_pgw:
                    leak_from, leak_to = "Routing aggregation", "RoutingPGW"
                    val_from, val_to   = out_aggr, out_pgw
                elif out_pgw > item['n_ok']:
                    leak_from, leak_to = "RoutingPGW", "DWH/Final"
                    val_from, val_to   = out_pgw, item['n_ok']
                else:
                    leak_from, leak_to = "Inconnu", "Inconnu"
                    
                print(f"{prefix}out {leak_from.ljust(21)}: {int(val_from)}")
                print(f"{prefix}out {leak_to.ljust(21)}: {int(val_to)}")

            print(f"{prefix}P(S_Out_OK)   : {item['kpi_succes']:.4f} ({item['kpi_succes']*100:.2f}%)")
            print(f"{prefix}P(S_Reject)   : {item['kpi_rejet']:.4f}  ({item['kpi_rejet']*100:.2f}%)")
            print(f"{prefix}P(S_Leak)     : {item['kpi_fuite']:.4f}  ({item['kpi_fuite']*100:.2f}%)")
            print(f"{prefix}Somme         : {somme:.6f}  [{statut}]")
            print()


if __name__ == "__main__":
    main()