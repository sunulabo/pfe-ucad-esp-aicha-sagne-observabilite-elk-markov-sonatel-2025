from enum import Enum


class EnumRole(Enum):
    """Rôles des utilisateurs dans le système."""
    CLIENT = "CLIENT"
    ANALYSTE = "ANALYSTE"
    EXPLOITATION = "D'EXPLOITATION"
    DECISIONNAIRE = "DECISIONNAIRE"
    ADMINISTRATEUR = "ADMINISTRATEUR"


class EnumTable(Enum):
    """Types de tables d'audit dans le pipeline de traitement."""
    AUDIT_COLLECT = "AUDIT_COLLECT"
    AUDIT_PROCESSING_IN = "AUDIT_PROCESSING_IN"
    AUDIT_PROCESSING_OUT = "AUDIT_PROCESSING_OUT"
    AUDIT_FILTER = "AUDIT_FILTER"
    AUDIT_FORWARDING = "AUDIT_FORWARDING"


class EnumStatut(Enum):
    """État d'un incident."""
    RESOLU = "RESOLU"
    EN_COURS = "EN_COURS"
    ASSIGNE = "ASSIGNE"
