"""
Moteur Chaînes de Markov — Surveillance flux MediationZone
PFE Aïcha SAGNE — ESP/UCAD / Sonatel 2025

Usage:
    python markov_chain.py --flux ggsn --window 30
    python markov_chain.py --flux msc  --window 30
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List

from elasticsearch import Elasticsearch

NOMINAL_MATRICES = {
    "ggsn": {
        "Filter":               {"toDupUDRPGW": 0.999},
        "Routing aggregation":  {"singleRecord": 1.0},
        "RoutingPGW": {
            "toOSN_DWH_NM5G":    0.517,
            "toOSN_MVNO_NM4G":   0.160,
            "toOSN_INTERCO_PGW": 0.147,
            "toOSN_MVNO_NM":     0.139,
            "toOSN_NESSICO4G":   0.017,
            "toOSN_DWH_RON":     0.009,
            "toOSN_NESSICO":     0.006,
            "toOGB_DWH_RI":      0.002,
            "toOSN_DWH_RM4G":    0.002,
        },
    },
    "msc": {
        "E1_ENRICHMENT": {"toECS": 0.958, "toLookup": 0.042},
        "LookupPorta":   {"toRouting": 1.0},
        "E4_ROUTING": {
            "to_LMS":       0.438,
            "DWH_NM":       0.327,
            "to_ICTCDREN":  0.097,
            "to_ICTCDRSO":  0.090,
            "toMVNONM":     0.043,
            "DWH_NM_CLONE": 0.004,
            "toDWhRM":      0.0002,
        },
    },
}


class MarkovEngine:

    def __init__(self, es: Elasticsearch, flux: str, window_days: int = 30):
        self.es = es
        self.flux = flux.lower()
        self.window_days = window_days
        self.nominal = NOMINAL_MATRICES.get(self.flux, {})

    def fetch_events(self) -> List[dict]:
        index_map = {
            "ggsn": "audit_markov_event-proc*",
            "msc":  "audit_markov_event-proc-mobile*",
        }
        since = datetime.utcnow() - timedelta(days=self.window_days)
        resp = self.es.search(
            index=index_map[self.flux],
            body={
                "query": {"range": {"process_start_time": {"gte": since.isoformat()}}},
                "_source": ["source", "destination", "details", "filename"],
                "size": 10000,
            }
        )
        return [h["_source"] for h in resp["hits"]["hits"]]

    def estimate_matrix(self, events: List[dict]) -> Dict[str, Dict[str, float]]:
        counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for ev in events:
            src, dst = ev.get("source", ""), ev.get("destination", "")
            if src and dst and src != "-":
                counts[src][dst] += 1
        return {
            src: {dst: cnt / sum(dsts.values()) for dst, cnt in dsts.items()}
            for src, dsts in counts.items()
        }

    def detect_anomalies(self, observed: Dict) -> List[dict]:
        anomalies = []
        for src, dsts_nom in self.nominal.items():
            dsts_obs = observed.get(src, {})
            for dst, p_nom in dsts_nom.items():
                p_obs = dsts_obs.get(dst, 0.0)
                score = abs(p_obs - p_nom) / p_nom if p_nom > 0 else 0
                if score > 0.20:
                    anomalies.append({
                        "source": src, "destination": dst,
                        "anomaly_score": round(score, 4),
                        "p_nominal": round(p_nom, 4),
                        "p_observed": round(p_obs, 4),
                        "severity": "HIGH" if score > 0.50 else "MEDIUM",
                        "flux": self.flux,
                        "detected_at": datetime.utcnow().isoformat(),
                    })
        return sorted(anomalies, key=lambda x: x["anomaly_score"], reverse=True)

    def compute_kpis(self, events: List[dict]) -> dict:
        n_total  = len(events)
        n_reject = sum(1 for e in events if e.get("destination") == "toECS")
        n_ok     = n_total - n_reject
        return {
            "flux": self.flux, "window_days": self.window_days,
            "n_total": n_total, "n_ok": n_ok, "n_reject": n_reject,
            "kpi_succes": round(n_ok / n_total, 4) if n_total else 0,
            "kpi_rejet":  round(n_reject / n_total, 4) if n_total else 0,
            "kpi_fuite":  0.0,
            "computed_at": datetime.utcnow().isoformat(),
        }

    def run(self) -> dict:
        print(f"\n[MarkovEngine] Flux={self.flux.upper()} | Fenêtre={self.window_days}j")
        events = self.fetch_events()
        print(f"  Événements : {len(events)}")
        observed = self.estimate_matrix(events)
        anomalies = self.detect_anomalies(observed)
        kpis = self.compute_kpis(events)
        if anomalies:
            print(f"  ⚠️  {len(anomalies)} anomalie(s) :")
            for a in anomalies[:3]:
                print(f"     [{a['severity']}] {a['source']} → {a['destination']} | score={a['anomaly_score']:.1%}")
        else:
            print("  ✅ Flux nominal")
        print(f"  KPIs : succès={kpis['kpi_succes']:.1%} | rejet={kpis['kpi_rejet']:.1%}")
        return {"kpis": kpis, "observed_matrix": observed, "anomalies": anomalies}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flux",    required=True, choices=["ggsn", "msc"])
    parser.add_argument("--window",  type=int, default=30)
    parser.add_argument("--es-host", default="http://localhost:9200")
    parser.add_argument("--es-user", default="elastic")
    parser.add_argument("--es-pass", default="changeme")
    parser.add_argument("--output",  default=None)
    args = parser.parse_args()

    es = Elasticsearch(args.es_host, basic_auth=(args.es_user, args.es_pass), verify_certs=False)
    result = MarkovEngine(es, args.flux, args.window).run()

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        print(f"  Exporté : {args.output}")

if __name__ == "__main__":
    main()
