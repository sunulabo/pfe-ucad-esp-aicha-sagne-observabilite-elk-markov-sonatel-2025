from datetime import datetime


class Flux:
    def __init__(self, idFlux: str, nomFlux: str, equationEtancheite: str = None):
        self.idFlux = idFlux
        self.nomFlux = nomFlux
        self.equationEtancheite = equationEtancheite
        self.dateCreation = datetime.utcnow()

    def __repr__(self):
        return f"Flux(id={self.idFlux}, nom={self.nomFlux})"
