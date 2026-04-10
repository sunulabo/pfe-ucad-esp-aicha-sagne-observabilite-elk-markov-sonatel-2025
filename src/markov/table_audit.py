from datetime import datetime
from enums import EnumTable


class TableAudit:
    def __init__(self, idTable: str, typeTable: EnumTable, details: dict):
        self.idTable = idTable
        self.typeTable = typeTable
        self.details = details
        self.dateCreation = datetime.utcnow()

    def __repr__(self):
        return f"TableAudit(id={self.idTable}, type={self.typeTable.value})"
