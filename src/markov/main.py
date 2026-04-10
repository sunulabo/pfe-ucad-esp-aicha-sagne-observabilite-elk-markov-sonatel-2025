import json
import logging
import os
from datetime import datetime

import pandas as pd

from chaine_markov import ChaineMarkov
from db_connection import fetch_recent_traces
from elastic_loader import ElasticLoader
from enums import EnumStatut
from incident import Incident
from rapport import Rapport
from systeme_ia import SystemeIA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("main")


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def transformer_etats(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    node_map = mapping.get("nodes", {})
    out = df.copy()
    out["etat_source"] = out["source_node"].map(node_map).fillna("UNKNOWN")
    out["etat_arrivee"] = out["dest_node"].map(node_map).fillna("UNKNOWN")
    return out


def charger_baseline(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    return load_json(path)


def parse_datetime_env(value: str):
    if not value:
        return None
    return datetime.fromisoformat(value)


def _load_runtime_settings() -> dict:
    baseline_path = os.getenv("MARKOV_BASELINE_PATH", "")
    return {
        "window_minutes": int(os.getenv("MARKOV_WINDOW_MINUTES", "5")),
        "start_time": parse_datetime_env(os.getenv("MARKOV_START_TIME", "")),
        "end_time": parse_datetime_env(os.getenv("MARKOV_END_TIME", "")),
        "leak_threshold": float(os.getenv("MARKOV_LEAK_THRESHOLD", "0.0")),
        "deviation_tolerance": float(os.getenv("MARKOV_DEVIATION_TOLERANCE", "0.05")),
        "baseline": charger_baseline(baseline_path),
    }


def _process_incidents(systeme: SystemeIA, incidents: list, stats: list) -> None:
    for inc_data in incidents:
        now = datetime.utcnow()
        incident = Incident(
            idIncident=f"INC_{inc_data.get('flux','NA')}_{int(now.timestamp())}",
            type=inc_data.get("type", "ANOMALIE"),
            statut=EnumStatut.ASSIGNE,
            dateDetection=now,
        )
        systeme.envoyerAlertes(incident)

        related = next(
            (
                s
                for s in stats
                if s.get("flux") == inc_data.get("flux")
                and s.get("filename") == inc_data.get("filename")
                and s.get("etat") == inc_data.get("etat")
            ),
            {},
        )
        faits = systeme.collecterFaitsPertinents(inc_data.get("flux", "UNKNOWN"), related)
        explication = systeme.genererExplicationRAG(faits)

        rapport = Rapport(
            idRapport=f"RPT_{inc_data.get('flux','NA')}_{int(now.timestamp())}",
            type="MARKOV_DIAGNOSTIC",
            contenu={"incident": inc_data, "explication": explication, "faits": faits},
        )
        logger.info("Rapport genere: %s", rapport.genererRapport())


def _push_stats_to_elastic(stats: list) -> None:
    es_hosts = [h.strip() for h in os.getenv("ELASTIC_HOSTS", "").split(",") if h.strip()]
    if not es_hosts:
        logger.warning("ELASTIC_HOSTS non defini: chargement Elasticsearch ignore")
        return

    logger.info("Chargement des KPI dans Elasticsearch")
    loader = ElasticLoader(
        hosts=es_hosts,
        username=os.getenv("ELASTIC_USER", ""),
        password=os.getenv("ELASTIC_PASSWORD", ""),
        verify_certs=os.getenv("ELASTIC_VERIFY_CERTS", "false").lower() == "true",
    )
    index_name = os.getenv("ELASTIC_INDEX", "flux-markov-stats")
    loader.envoyer_documents(index_name, stats)


def main() -> None:
    db_config = load_json("db_config.json")
    mapping = load_json("config_mapping.json")
    settings = _load_runtime_settings()

    logger.info(
        "Extraction SQL via tables configurees dans db_config.json (window=%s, start=%s, end=%s)",
        settings["window_minutes"],
        settings["start_time"],
        settings["end_time"],
    )
    df = fetch_recent_traces(
        db_config,
        window_minutes=settings["window_minutes"],
        start_time=settings["start_time"],
        end_time=settings["end_time"],
    )
    if df.empty:
        logger.info("Aucune trace recente a traiter")
        return

    logger.info("Transformation via config_mapping.json")
    df_markov = transformer_etats(df, mapping)

    chaine = ChaineMarkov(idMarkov="Audit_Sonatel", Etats=["S_In", "S_Proc", "S_Out_OK", "S_Reject", "S_Leak"])
    stats = chaine.calculerProbabilites(df_markov)
    if not stats:
        logger.info("Aucune statistique calculee")
        return

    systeme = SystemeIA(idSysteme="SYS_IA_01", contexte="Supervision Markov", llmLocal=True)
    incidents = chaine.detecterDeviation(
        stats,
        seuil_fuite=settings["leak_threshold"],
        baseline=settings["baseline"],
        tolerance=settings["deviation_tolerance"],
    )
    _process_incidents(systeme, incidents, stats)

    _push_stats_to_elastic(stats)


if __name__ == "__main__":
    main()
