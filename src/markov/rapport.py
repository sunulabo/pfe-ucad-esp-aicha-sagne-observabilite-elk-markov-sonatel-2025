from datetime import datetime
import json


class Rapport:
    def __init__(self, idRapport: str, type: str, contenu: dict, date: datetime = None):
        self.idRapport = idRapport
        self.type = type
        self.contenu = contenu
        self.date = date or datetime.utcnow()

    def genererRapport(self) -> str:
        rapport = {
            'idRapport': self.idRapport,
            'type': self.type,
            'date': self.date.isoformat(),
            'contenu': self.contenu,
        }
        return json.dumps(rapport, indent=2, ensure_ascii=False)

    def __repr__(self):
        return f"Rapport(id={self.idRapport}, type={self.type})"
