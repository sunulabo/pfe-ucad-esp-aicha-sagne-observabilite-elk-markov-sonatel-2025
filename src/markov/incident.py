from datetime import datetime
from enums import EnumStatut
from utilisateur import Utilisateur


class Incident:
    def __init__(self, idIncident: str, type: str, statut: EnumStatut, dateDetection: datetime = None, assignation: Utilisateur = None):
        self.idIncident = idIncident
        self.type = type
        self.statut = statut
        self.dateDetection = dateDetection or datetime.utcnow()
        self.assignation = assignation
        self.notes = []

    def resoudreIncident(self, noteResolution: str = ""):
        self.statut = EnumStatut.RESOLU
        if noteResolution:
            self.notes.append(f"[{datetime.utcnow().isoformat()}] RESOLUTION: {noteResolution}")

    def __repr__(self):
        return f"Incident(id={self.idIncident}, type={self.type}, statut={self.statut.value})"
