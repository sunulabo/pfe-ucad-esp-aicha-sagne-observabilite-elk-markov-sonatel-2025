#!/usr/bin/env python3
"""
============================================================
 push_markov_to_elastic.py — UADB Master 2
 Version locale : MySQL + ELK local
 Enrichi avec : udr_key, source, destination, details,
                etape, doc_type (kpi / transition / agent_kpi)
============================================================
"""

import argparse
import json
import os
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from chaine_markov import ChaineMarkov
from elastic_loader import ElasticLoader
from connexion_bd import _compute_agent_audit_v2

# ── Fenêtres par défaut ────────────────────────────────────
DEFAULT_WINDOW_MOBILE = (
    datetime.fromisoformat("2026-02-27T08:45:56.024"),
    datetime.fromisoformat("2026-03-04T09:11:14.998"),
)
DEFAULT_WINDOW_GGSN = (
    datetime.fromisoformat("2026-03-11T18:32:02.959"),
    datetime.fromisoformat("2026-03-12T16:41:31.450"),
)

# ── Normalisation flux ─────────────────────────────────────
_FLUX_NORM = {"msc": "mobile", "data": "ggsn"}
_FLUX_TAGS = {
    "mobile": ["MOBILE", "MSC"],
    "msc":    ["MSC", "MOBILE"],
    "ggsn":   ["GGSN", "DATA"],
    "data":   ["DATA", "GGSN"],
}

# ── Mapping nodes → états Markov ───────────────────────────
NODE_MAPPING = {
    "toProcessingCollect": "S_In",
    "toArchive":           "S_In",
    "toProcessingSGW":     "S_In",
    "toProcessingPGW":     "S_In",
    "toArchivePGW":        "S_In",
    "toUnDecodable":       "S_In",
    "toProcessing":        "S_In",
    "toDupUDRPGW":         "S_Proc",
    "toRouteAggregation":  "S_Proc",
    "singleRecord":        "S_Proc",
    "toLookup":            "S_Proc",
    "toMZTagEnc":          "S_Proc",
    "toProcessing2":       "S_Proc",
    "toDupUDR":            "S_Proc",
    "toRoute":             "S_Proc",
    "toRouting":           "S_Proc",
    "toEnrichment":        "S_Proc",
    "toAnalysis":          "S_Proc",
    "E2_DUP_UDR":          "S_Proc",
    "toOSN_DWH_NM":        "S_Out_OK",
    "toOSN_DWH_NM4G":      "S_Out_OK",
    "DWH_NM":              "S_Out_OK",
    "DWH_NM_CLONE":        "S_Out_OK",
    "toOSN_INTERCO_PGW":   "S_Out_OK",
    "toMVNONM":            "S_Out_OK",
    "to_LMS":              "S_Out_OK",
    "to_ICTCDREN":         "S_Out_OK",
    "to_ICTCDRSO":         "S_Out_OK",
    "toECS":               "S_Out_OK",
    "toOSN_MVNO_NM4G":     "S_Out_OK",
    "toOSN_NESSICO4G":     "S_Out_OK",
    "toOSN_NESSICO5G":     "S_Out_OK",
    "toOSN_NESSICO":       "S_Out_OK",
    "toOSN_MVNO_NM":       "S_Out_OK",
    "toOSN_DWH_NM5G":      "S_Out_OK",
    "toOSN_DWH_RM4G":      "S_Out_OK",
    "toOGB_DWH_RI":        "S_Out_OK",
    "toOSN_DWH_RON":       "S_Out_OK",
    "REJECT_NO_APPLI":     "S_Reject",
    "ELIMINE_NO_APPLI":    "S_Reject",
    "to_ELIMINE_INEFF":    "S_Reject",
    "to_ELIMINE_MISSING":  "S_Reject",
    "to_ELIMINE_INVALID":  "S_Reject",
    "toHandlerMISSING":    "S_Reject",
    "toHandlerMISSING2":   "S_Reject",
    "toHandlerINVALID":    "S_Reject",
    "toDuplicatePGW":      "S_Reject",
    "toPartialRecord":     "S_Reject",
    "toElimineMissing":    "S_Reject",
    "toElimineInvalid":    "S_Reject",
    "toElimineIneff":      "S_Reject",
    "toElimineNoAppli":    "S_Reject",
    "toDuplicate":         "S_Reject",
}


# ─────────────────────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────────────────────

def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _parse_datetime(value: str):
    if not value:
        return None
    return datetime.fromisoformat(value)

def parse_workflow(workflow_name: str) -> dict:
    wf = workflow_name or ""
    flux = "MSC" if "MSC" in wf else "GGSN" if ("GGSN" in wf or "PGW" in wf) else "UNKNOWN"
    match = re.search(r'WFL_([^.]+)\.', wf)
    if match:
        wfl = match.group(1).lower()
        if "processingpgw" in wfl or "processing" in wfl:
            etape = "Processing"
        elif "collection" in wfl or "collect" in wfl:
            etape = "Collection"
        elif "preprocess" in wfl:
            etape = "Preprocessing"
        elif "aggreg" in wfl:
            etape = "Preaggregation"
        else:
            etape = match.group(1).split("_")[0].capitalize()
    else:
        etape = "Unknown"
    return {"flux": flux, "etape": etape}

def node_state(node: str) -> str:
    return NODE_MAPPING.get(node, "S_Unknown")

def get_mysql_conn(db_entry: dict):
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=db_entry.get("host", "localhost"),
            port=int(db_entry.get("port", 3306)),
            database=db_entry.get("dbname", "audit_markov"),
            user=db_entry.get("user", "root"),
            password=db_entry.get("password", ""),
        )
        return conn
    except ImportError:
        raise ImportError("mysql-connector-python non installé.")

# ─────────────────────────────────────────────────────────────
# Extraction par Stream et Générateur
# ─────────────────────────────────────────────────────────────

def stream_elastic_docs(
    db_entry: dict, start_time: datetime, end_time: datetime, 
    source_filter: str, flux_tags: list, flux_arg: str, is_ggsn: bool
):
    """
    Générateur (yield) des documents à envoyer vers Elasticsearch.
    Permet de contourner les Crash/Out of Memory en listes Python.
    """
    conn = get_mysql_conn(db_entry)
    cur = conn.cursor(dictionary=True)

    table = "audit_markov_event_proc_ggsn" if is_ggsn else "audit_markov_event_proc"
    
    try:
        cur.execute("SET SESSION net_read_timeout=3600")
        cur.execute("SET SESSION net_write_timeout=3600")
        cur.execute("SET SESSION wait_timeout=3600")
    except:
        pass

    query = f"""
        SELECT workflow_name, udr_key, source, destination, details, filename, fileid, process_start_time, process_end_time
        FROM {table}
        WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
          AND source IS NOT NULL AND destination IS NOT NULL
    """
    cur.execute(query, (start_time, end_time))

    ts = datetime.utcnow().isoformat()
    
    file_stats = defaultdict(lambda: {
        "n_proc": 0, "n_ok": 0, "n_reject": 0,
        "workflow_name": "", "filename": "", "fileid": "",
        "etape": "", "flux": "",
        "window_start": None, "window_end": None,
        "udr_proc": set(), "udr_ok": set(), "udr_reject": set(),
    })
    path_stats = defaultdict(lambda: {"n_udr": 0, "workflow": "", "details_sample": ""})
    agent_stats = defaultdict(lambda: {"IN": set(), "OUT": set()})
    
    S_OUT_OK = {k for k, v in NODE_MAPPING.items() if v == "S_Out_OK"}
    S_REJECT = {k for k, v in NODE_MAPPING.items() if v == "S_Reject"}
    S_PROC   = {k for k, v in NODE_MAPPING.items() if v == "S_Proc"}

    # --- 1) Stream UDR ---
    print("\n   => Démarrage du streaming SQL des traces UDR ...")
    while True:
        rows = cur.fetchmany(10000)
        if not rows:
            break
        
        for row in rows:
            workflow = row.get("workflow_name", "") or ""
            udr_key  = row.get("udr_key", "")       or ""
            source   = row.get("source", "")         or ""
            dest     = row.get("destination", "")    or ""
            details  = row.get("details", "")        or ""
            filename = row.get("filename", "")       or ""
            fileid   = row.get("fileid", "")         or ""
            w_start  = row.get("process_start_time")
            w_end    = row.get("process_end_time")

            if not udr_key or not source or not dest or not filename:
                continue

            key = (workflow, filename)
            g   = file_stats[key]
            g["workflow_name"] = workflow
            g["filename"]      = filename
            g["fileid"]        = fileid

            wf_info = parse_workflow(workflow)
            if not g["etape"]:
                g["etape"] = wf_info["etape"]
                g["flux"]  = wf_info["flux"]

            if w_start and (g["window_start"] is None or str(w_start) < str(g["window_start"])):
                g["window_start"] = str(w_start)
            if w_end and (g["window_end"] is None or str(w_end) > str(g["window_end"])):
                g["window_end"] = str(w_end)

            if source == source_filter and udr_key not in g["udr_proc"]:
                g["udr_proc"].add(udr_key)
                g["n_proc"] += 1
            if dest in S_OUT_OK and udr_key not in g["udr_ok"]:
                g["udr_ok"].add(udr_key)
                g["n_ok"] += 1
            if dest in S_REJECT and udr_key not in g["udr_reject"]:
                g["udr_reject"].add(udr_key)
                g["n_reject"] += 1

            path_key  = (wf_info["etape"], wf_info["flux"], workflow, source, dest)
            path_stats[path_key]["n_udr"] += 1
            path_stats[path_key]["workflow"] = workflow
            path_stats[path_key]["details_sample"] = details

            # Tracking V2 IN/OUT en mémoire pour l'audit des agents
            base_key = udr_key.rsplit('-', 1)[0] if '-' in udr_key else udr_key
            if dest:
                agent_stats[(workflow, filename, dest)]["IN"].add(base_key)
            if source:
                agent_stats[(workflow, filename, source)]["OUT"].add(base_key)

            # YIELD DOCUMENT UDR
            doc_id = f"udr_{udr_key}_{source}_{uuid.uuid4().hex[:6]}"
            is_leak_udr = node_state(dest) not in ("S_Out_OK", "S_Reject", "S_Proc", "S_In")
            yield {
                "_id": doc_id,
                "doc_type":      "udr",
                "@timestamp":    ts,
                "etape":         wf_info["etape"],
                "flux":          flux_tags,
                "workflow_name": workflow,
                "filename":      filename,
                "udr_key":       udr_key,
                "source":        source,
                "destination":   dest,
                "details":       details,
                "src_state":     node_state(source),
                "dst_state":     node_state(dest),
                "is_leak":       is_leak_udr,
                "leak_stage":    source if is_leak_udr else None,
                "window_start":  str(w_start) if w_start else None,
                "window_end":    str(w_end)   if w_end   else None,
            }

    cur.close()

    print("\n   => Calcul du diagnostic (Agent V2) très rapide en mémoire Python ...")
    import pandas as pd
    agents_data_list = []
    for (wf, fn, ag), io_dict in agent_stats.items():
        if not wf or not fn or not ag:
            continue
        n_in = len(io_dict["IN"])
        n_out = len(io_dict["OUT"])
        n_leak_int = max(0, n_in - n_out)
        p_leak_int = n_leak_int / max(1, n_in)
        agents_data_list.append({
            "workflow_name": wf,
            "filename": fn,
            "agent_name": ag,
            "IN": n_in,
            "OUT": n_out,
            "n_leak_internal": n_leak_int,
            "p_leak_internal": p_leak_int
        })
    df_agents = pd.DataFrame(agents_data_list) if agents_data_list else pd.DataFrame()

    # --- 2) Yield KPIs ---
    print("\n   => Streaming des KPIs (Fichiers, Transitions)...")
    for (workflow, filename), g in file_stats.items():
        n_proc   = g["n_proc"]
        n_ok     = g["n_ok"]
        n_reject = g["n_reject"]

        if n_proc == 0 and (n_ok > 0 or n_reject > 0):
            n_proc = n_ok + n_reject
        if n_proc == 0:
            continue

        n_leak   = max(0, n_proc - n_ok - n_reject)
        p_ok     = round(min(1.0, n_ok     / n_proc), 6)
        p_reject = round(min(1.0, n_reject / n_proc), 6)
        p_leak   = round(max(0.0, 1.0 - p_ok - p_reject), 6)

        is_aggregate = not bool(re.match(r'^\d+_', filename))
        
        leak_step_from = None
        leak_step_to   = None
        leak_out_from  = 0
        leak_out_to    = 0
        if n_leak > 0:
            if g["flux"] == "GGSN" and not df_agents.empty:
                agents_for_file = df_agents[df_agents["filename"] == filename]
                def get_out(a_name):
                    m = agents_for_file[agents_for_file["agent_name"] == a_name]
                    return m["OUT"].sum() if not m.empty else 0
                
                out_filter = get_out("Filter")
                out_aggr   = get_out("Routing aggregation")
                out_pgw    = get_out("RoutingPGW")
                
                if out_filter > out_aggr:
                    leak_step_from = "Filter"
                    leak_step_to   = "Routing aggregation"
                    leak_out_from  = int(out_filter)
                    leak_out_to    = int(out_aggr)
                elif out_aggr > out_pgw:
                    leak_step_from = "Routing aggregation"
                    leak_step_to   = "RoutingPGW"
                    leak_out_from  = int(out_aggr)
                    leak_out_to    = int(out_pgw)
                elif out_pgw > n_ok:
                    leak_step_from = "RoutingPGW"
                    leak_step_to   = "DWH/Final"
                    leak_out_from  = int(out_pgw)
                    leak_out_to    = int(n_ok)
                else:
                    leak_step_from = "Unknown"
                    leak_step_to   = "Unknown"
            else:
                leak_step_from = "routing"
                leak_step_to   = "Unknown"

        yield {
            "_id": f"{flux_arg.upper()}_{workflow}_{filename}",
            "doc_type":      "kpi",
            "@timestamp":    ts,
            "etape":         g["etape"],
            "flux":          flux_tags,
            "workflow_name": workflow,
            "filename":      filename,
            "fileid":        g["fileid"],
            "is_aggregate":  is_aggregate,
            "window_start":  g["window_start"] or start_time.isoformat(),
            "window_end":    g["window_end"]   or end_time.isoformat(),
            "n_proc":        n_proc,
            "n_ok":          n_ok,
            "n_reject":      n_reject,
            "n_leak":        n_leak,
            "is_leak":       bool(n_leak > 0),
            "kpi_succes":    p_ok,
            "kpi_rejet":     p_reject,
            "kpi_fuite":     p_leak,
            "source":        "Filter" if "GGSN" in str(flux_tags) else "E1_FILTER",
            "destination":   "toDupUDRPGW" if "GGSN" in str(flux_tags) else "toDupUDR",
            "leak_step_from": leak_step_from,
            "leak_out_from":  leak_out_from,
            "leak_step_to":   leak_step_to,
            "leak_out_to":    leak_out_to,
            "leak_msg_from":  f"out {leak_step_from} : {leak_out_from}" if leak_step_from and leak_step_from != "Unknown" else None,
            "leak_msg_to":    f"out {leak_step_to}   : {leak_out_to}" if leak_step_to and leak_step_to != "Unknown" else None,
        }

    for (etape, flux, workflow, source, dest), stats in path_stats.items():
        n_udr     = stats["n_udr"]
        dst_state = node_state(dest)
        is_leak   = (dst_state not in ("S_Out_OK", "S_Reject", "S_Proc", "S_In") and dest not in S_OUT_OK and dest not in S_REJECT and dest not in S_PROC)
        
        yield {
            "_id": f"path_{etape}_{flux}_{source}_{dest}_{uuid.uuid4().hex[:6]}",
            "doc_type":      "transition",
            "@timestamp":    ts,
            "etape":         etape,
            "flux":          flux_tags,
            "workflow_name": workflow,
            "source":        source,
            "destination":   dest,
            "details":       stats["details_sample"],
            "src_state":     node_state(source),
            "dst_state":     dst_state,
            "n_udr":         n_udr,
            "n_leak":        n_udr if is_leak else 0,
            "pct_leak":      round((n_udr if is_leak else 0) / n_udr * 100, 2) if n_udr > 0 else 0.0,
            "is_leak":       is_leak,
            "leak_stage":    source if is_leak else None,
            "window_start":  start_time.isoformat(),
            "window_end":    end_time.isoformat(),
        }

    # --- 3) YIELD AUDIT V2 (Agent KPI) ---
    print("\n   => Streaming des Agent KPIs (V2) via REGEXP_REPLACE ...")
    
    if not df_agents.empty:
        for _, row in df_agents.iterrows():
            workflow_name = row["workflow_name"]
            filename = row["filename"]
            agent_name = row["agent_name"]
            n_in = int(row["IN"])
            n_out = int(row["OUT"])
            n_leak = int(row["n_leak_internal"])
            p_leak = float(row["p_leak_internal"])
            
            wf_info = parse_workflow("WFL_" + filename)
            
            yield {
                "_id": f"agent_{agent_name}_{filename}",
                "doc_type":        "agent_kpi",       
                "@timestamp":      ts,                
                "etape":           wf_info["etape"],             
                "flux":            flux_tags,         
                "workflow_name":   row["workflow_name"],
                "filename":        filename,
                "agent_name":      agent_name,        
                "n_in":            n_in,              
                "n_out":           n_out,             
                "n_leak_internal": n_leak,
                "is_leak":         bool(n_leak > 0),
                "p_leak_internal": p_leak,   
                "window_start":    start_time.isoformat(),
                "window_end":      end_time.isoformat(),
            }

    conn.close()

# ─────────────────────────────────────────────────────────────
# Arguments CLI
# ─────────────────────────────────────────────────────────────

def _build_args():
    parser = argparse.ArgumentParser(description="Push Markov → Elasticsearch (local)")
    parser.add_argument("--db-config",      default="db_config.json")
    parser.add_argument("--db-name",        default="")
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--start-time",     default="")
    parser.add_argument("--end-time",       default="")
    parser.add_argument("--elastic-config", default="elastic_config.json")
    parser.add_argument("--flux", choices=["mobile", "msc", "ggsn", "data"], default="ggsn")
    parser.add_argument("--es-hosts",    default=os.getenv("ELASTIC_HOSTS",    ""))
    parser.add_argument("--es-user",     default=os.getenv("ELASTIC_USER",     ""))
    parser.add_argument("--es-password", default=os.getenv("ELASTIC_PASSWORD", ""))
    parser.add_argument("--es-index",    default=os.getenv("ELASTIC_INDEX",    ""))
    return parser.parse_args()

def _build_elastic_connection(args, es_cfg):
    es_hosts_arg = [h.strip() for h in args.es_hosts.split(",") if h.strip()]
    if es_hosts_arg: hosts = es_hosts_arg
    elif es_cfg.get("host"): hosts = [f"{str(es_cfg['host']).rstrip('/')}:{es_cfg.get('port', 9200)}"]
    else: hosts = ["http://localhost:9200"]

    username     = args.es_user     or es_cfg.get("username", "elastic")
    password     = args.es_password or es_cfg.get("password", "UADB@BigData2025!")
    verify_certs = bool(es_cfg.get("verify_certs", False)) if es_cfg.get("ssl_certificate_verification") is not False else False
    index        = args.es_index or es_cfg.get("index", "etats-markov")
    return hosts, username, password, verify_certs, index

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    args = _build_args()

    flux_norm = _FLUX_NORM.get(args.flux.lower(), args.flux.lower())
    is_ggsn   = (flux_norm == "ggsn")
    flux_tags = _FLUX_TAGS.get(args.flux.lower(), [args.flux.upper()])
    source_filter = "Filter" if is_ggsn else "E1_FILTER"

    start_time = _parse_datetime(args.start_time)
    end_time   = _parse_datetime(args.end_time)

    if start_time is None and end_time is None:
        start_time, end_time = DEFAULT_WINDOW_GGSN if is_ggsn else DEFAULT_WINDOW_MOBILE
    elif start_time is None:
        start_time = end_time - timedelta(minutes=args.window_minutes)
    elif end_time is None:
        end_time = start_time + timedelta(minutes=args.window_minutes)

    print(f"[1/3] Configuration validée")
    print(f"      Flux={flux_tags} | source_filter={source_filter}")
    print(f"      Fenetre : {start_time} --> {end_time}")

    db_config = _load_json(args.db_config)
    if args.db_name:
        db_config["databases"] = [db for db in db_config.get("databases", []) if db.get("name") == args.db_name]
    if not db_config.get("databases"):
        print("Erreur : Base de données non trouvée dans db_config.json")
        return
    db_entry = db_config["databases"][0]

    print(f"\n[2/3] Connexion Elasticsearch locale...")
    es_cfg = _load_json(args.elastic_config) if os.path.exists(args.elastic_config) else {}
    hosts, username, password, verify_certs, target_index = _build_elastic_connection(args, es_cfg)
    loader = ElasticLoader(hosts=hosts, username=username, password=password, verify_certs=verify_certs)

    print(f"\n[3/3] Extraction des traces et envoi Elasticsearch (En continu)")
    doc_generator = stream_elastic_docs(
        db_entry, start_time, end_time, source_filter, flux_tags, args.flux, is_ggsn
    )
    
    sent = loader.envoyer_documents(target_index, doc_generator, batch_size=2000)
    
    print(f"\n{'='*60}")
    print(f" TERMINÉ ! {sent} documents propulsés.")
    print(f" Kibana → http://localhost:5601")
    print(f" Index  → {target_index}")
    print(f"   doc_type: agent_kpi        → < NOUVEAU > KPIs méticuleux par agent (V2)")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()