from enums import EnumRole


class Utilisateur:

    def __init__(self, id: str, nom: str, email: str, motDePasse: str, role: EnumRole):
        self.id = id
        self.nom = nom
        self.email = email
        self.motDePasse = motDePasse
        self.role = role

    def __repr__(self):
        return f"Utilisateur(id={self.id}, nom={self.nom}, email={self.email}, role={self.role.value})"
