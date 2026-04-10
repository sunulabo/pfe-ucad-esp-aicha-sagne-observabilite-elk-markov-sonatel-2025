import logging
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger('systeme_ia')


class SystemeIA:

    def __init__(self, idSysteme: str, contexte: str, llmLocal: bool = True):
        self.idSysteme = idSysteme
        self.contexte = contexte
        self.llmLocal = llmLocal
        self.resultats = []

    def envoyerAlertes(self, incident: Any) -> bool:
        try:
            logger.warning(f"ALERTE: {incident}")
            self.resultats.append({'type': 'alerte', 'incident': incident, 'ts': datetime.utcnow().isoformat()})
            return True
        except Exception:
            logger.exception('Erreur lors de l envoi d alerte')
            return False

    def collecterFaitsPertinents(self, flux_id: str, stats: Dict[str, Any]) -> Dict[str, Any]:
        faits = {
            'flux': flux_id,
            'timestamp': datetime.utcnow().isoformat(),
            'total_out': stats.get('total_out', 0),
            'kpi_succes': stats.get('kpi_succes', 0.0),
            'kpi_rejet': stats.get('kpi_rejet', 0.0),
            'kpi_fuite': stats.get('kpi_fuite', 0.0),
            'missing_count': stats.get('missing_count', 0),
            'dests': stats.get('dests', {}),
        }
        self.resultats.append({'type': 'faits', 'flux': flux_id, 'faits': faits})
        return faits

    def genererExplicationRAG(self, faits: Dict[str, Any]) -> str:
        texte = (f"Flux {faits.get('flux')} | total:{faits.get('total_out')} | "
                 f"fuite:{faits.get('kpi_fuite',0.0):.2%} | missing:{faits.get('missing_count')}")
        if self.llmLocal:
            texte += ' | explication générée localement (RAG)'
        return texte
