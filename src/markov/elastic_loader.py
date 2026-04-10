import logging
from datetime import datetime
from typing import Dict, Iterable, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logger = logging.getLogger("elastic_loader")


class ElasticLoader:
    def __init__(self, hosts: Iterable[str], username: str = "",
                 password: str = "", verify_certs: bool = False):
        hosts = list(hosts)
        if not hosts:
            raise ValueError("Au moins un host Elasticsearch est requis")

        # S'assurer que chaque host a un scheme
        hosts = [
            h if h.startswith("http") else f"http://{h}"
            for h in hosts
        ]

        kwargs = {
            "hosts": hosts, 
            "verify_certs": verify_certs,
            "request_timeout": 60,
            "retry_on_timeout": True,
            "max_retries": 3
        }
        if username and password:
            kwargs["basic_auth"] = (username, password)

        self.client = Elasticsearch(**kwargs)

    def envoyer_documents(self, index_name: str, documents: Iterable[Dict],
                          batch_size: int = 500) -> int:
        """
        Envoie les documents vers Elasticsearch via streaming_bulk.
        Beaucoup plus rapide que doc par doc sans exploser la mémoire.
        """
        from elasticsearch.helpers import streaming_bulk

        now  = datetime.utcnow().isoformat()
        sent = 0

        def _actions():
            for doc in documents:
                payload = dict(doc)
                payload.setdefault("@timestamp", now)
                doc_id = payload.pop("_id", None)
                action = {
                    "_index":    index_name,
                    "_source":   payload,
                }
                if doc_id:
                    action["_id"] = doc_id
                yield action

        try:
            for ok, result in streaming_bulk(self.client, _actions(), chunk_size=batch_size, max_retries=5, initial_backoff=2, raise_on_error=False):
                if ok:
                    sent += 1
                else:
                    logger.warning("Erreur Elastic sur 1 document : %s", result)
                
                if sent > 0 and sent % 10000 == 0:
                    print(f"      ... {sent:,} envoyés")
        except Exception as exc:
            logger.error("Erreur générale lors de l'envoi streaming : %s", exc)

        logger.info("%d documents envoyés vers l'index %s", sent, index_name)
        return sent