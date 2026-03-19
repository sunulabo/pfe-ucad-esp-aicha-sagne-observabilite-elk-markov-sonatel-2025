"""
Injection CSV Kibana Discover → Elasticsearch
PFE Aïcha SAGNE — ESP/UCAD / Sonatel 2025

Usage:
    python inject_data_elk.py --source ../../data/csv-exports/
"""

import argparse, csv, glob, os
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

INDEX_MAP = {
    "collect":     "audit_markov_event-collect",
    "proc-mobile": "audit_markov_event-proc-mobile",
    "proc":        "audit_markov_event-proc",
}

def parse_date(s):
    if not s or s == "-": return None
    for fmt in ["%b %d, %Y @ %H:%M:%S.%f", "%b  %d, %Y @ %H:%M:%S.%f"]:
        try: return datetime.strptime(s.strip(), fmt).isoformat()
        except: continue
    return s

def detect_index(path):
    fname = os.path.basename(path).lower()
    for key, idx in INDEX_MAP.items():
        if key in fname: return idx
    return "audit_markov_event-unknown"

def load_csv(path):
    docs, index = [], detect_index(path)
    with open(path) as f:
        for row in csv.DictReader(f):
            doc = {k.replace(".keyword","").strip("@"): v
                   for k, v in row.items()
                   if not k.endswith(".keyword") and k not in ["_score","_ignored"]}
            for df in ["process_start_time","process_end_time","@timestamp"]:
                if df in doc: doc[df] = parse_date(doc[df])
            for nf in ["output_count","part_count","timeout_count"]:
                if nf in doc:
                    try: doc[nf] = int(doc[nf])
                    except: doc[nf] = 0
            docs.append({"_index": index, "_source": doc})
    return docs

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source",  required=True)
    parser.add_argument("--es-host", default="http://localhost:9200")
    parser.add_argument("--es-user", default="elastic")
    parser.add_argument("--es-pass", default="changeme")
    args = parser.parse_args()

    es = Elasticsearch(args.es_host, basic_auth=(args.es_user, args.es_pass), verify_certs=False)
    total = 0
    for path in glob.glob(os.path.join(args.source, "*.csv")):
        docs = load_csv(path)
        ok, _ = helpers.bulk(es, docs, raise_on_error=False, stats_only=False)
        total += ok
        print(f"  {os.path.basename(path):50s} → {detect_index(path)} : {ok}/{len(docs)}")
    print(f"\n✅ Total : {total} documents injectés")

if __name__ == "__main__":
    main()
