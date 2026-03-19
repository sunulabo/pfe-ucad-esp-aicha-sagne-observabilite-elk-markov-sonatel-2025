# Plateforme d'Observabilité Unifiée — Garantie d'Étanchéité des Flux Télécoms

> **Projet de Fin d'Études — Master 2 Génie Informatique**
> École Supérieure Polytechnique (ESP) — Université Cheikh Anta Diop (UCAD) de Dakar
> Année universitaire 2024–2025

---

## Présentation

Ce dépôt contient les livrables du PFE de **Aïcha SAGNE**, réalisé au sein du service **DSI/EAI/MRI de Sonatel** (Groupe Orange Sénégal).

### Sujet

**De l'Ingestion des Tables d'Audit à l'Analyse de Cause Racine par LLM Souverain**

Conception et prototypage d'une plateforme d'observabilité intelligente pour la supervision des flux de médiation télécom (DigitalRoute MediationZone), combinant :

- **Stack ELK** (Elasticsearch 9.3 / Logstash / Kibana) — ingestion et visualisation
- **Chaînes de Markov** — modélisation comportementale des flux CDR/UDR
- **LLM Souverain** (Ollama/Mistral 7B + ChromaDB) via approche RAG — Analyse de Cause Racine

---

## Encadrement

| Rôle | Nom | Structure |
|------|-----|-----------|
| Encadreur industriel | **Ahmed Ben Sidy Bouya SEYE** | Lead MRI — DSI/EAI, Sonatel |
| Directeur de mémoire | *[à compléter]* | ESP/UCAD |
| Stagiaire | **Aïcha SAGNE** | M2 Génie Informatique, ESP/UCAD |

---

## Architecture

```
MediationZone EC1/EC2
        │
        ▼
Filebeat / Logstash ──► Elasticsearch (audit_markov_event-*)
                                 │
                   ┌─────────────┼─────────────┐
                   ▼             ▼              ▼
             Moteur Markov   Elastic ML    Kibana Dashboards
             (Python)        Anomaly Det.
                   │
                   ▼
        ChromaDB + Ollama/Mistral 7B
        (RAG — Analyse Cause Racine)
```

### Flux couverts

| Flux | Workflow MediationZone | Index ES | Événements |
|------|------------------------|----------|------------|
| GGSN/PGW 5G | `Proc_GGSN_63` | `audit_markov_event-proc` | 10 000 |
| MSC Voix/SMS | `Proc_MSC_H_01` | `audit_markov_event-proc-mobile` | 10 000 |
| Collecte | `WFL_Collection_hwei_1` | `audit_markov_event-collect` | 58 |

---

## Matrices de transition empiriques (données réelles Sonatel)

### Flux GGSN (Proc_GGSN_63) — split `4_0021364643`

| Source | Destination | Probabilité |
|--------|-------------|-------------|
| Filter | toDupUDRPGW | 47.5% |
| Routing aggregation | singleRecord | 47.9% |
| RoutingPGW | toOSN_DWH_NM5G | 51.7% |
| RoutingPGW | toOSN_MVNO_NM4G | 16.0% |
| RoutingPGW | toOSN_INTERCO_PGW | 14.7% |
| RoutingPGW | toOSN_MVNO_NM | 13.9% |

### Flux MSC (Proc_MSC_H_01) — fichiers `0000932188` + `0000932186`

| Source | Destination | Probabilité |
|--------|-------------|-------------|
| E1_ENRICHMENT | toECS (rejet) | **95.8%** |
| E1_ENRICHMENT | toLookup | 4.2% |
| LookupPorta | toRouting | 100% |
| E4_ROUTING | to_LMS | 43.8% |
| E4_ROUTING | DWH_NM | 32.7% |

> **Observation clé :** 99% des rejets ECS = `MSCH_RECY_TRUNK` → table TRUNK_MOBILE incomplète.

---

## Structure du dépôt

```
├── docs/
│   ├── memoire/              # Mémoire Word final
│   ├── uml/                  # Diagrammes UML (drawio + png)
│   └── fiches-textuelles/    # Fiches cas d'utilisation
├── stack/
│   ├── docker/               # docker-compose.yml ELK v9.3 + Ollama + ChromaDB
│   └── elk/
│       ├── logstash/         # Pipeline d'ingestion
│       ├── elasticsearch/    # Mappings, ILM policies
│       └── kibana/           # Exports dashboards (.ndjson)
├── src/
│   ├── markov/               # Moteur Chaînes de Markov
│   ├── injection/            # CSV Kibana → Elasticsearch
│   └── rag/                  # Pipeline RAG LLM souverain
├── data/
│   ├── csv-exports/          # Exports Kibana Discover
│   └── synthetic/            # Données synthétiques
├── notebooks/                # Analyse exploratoire
├── scripts/                  # Scripts utilitaires
└── tests/
```

---

## Démarrage rapide

```bash
# 1. Lancer la stack
cd stack/docker
cp .env.example .env
docker-compose up -d

# 2. Injecter les données
cd src/injection
pip install -r requirements.txt
python inject_data_elk.py --source ../../data/csv-exports/

# 3. Lancer le moteur Markov
cd src/markov
python markov_chain.py --flux ggsn --window 30
python markov_chain.py --flux msc  --window 30
```

---

## Mots-clés

`Télécoms` `Médiation` `MediationZone` `Chaînes de Markov` `Observabilité`
`ELK` `Elasticsearch` `Kibana` `LLM Souverain` `RAG` `Ollama` `Mistral`
`Revenue Assurance` `CDR` `UDR` `Sonatel` `Orange Sénégal` `ESP` `UCAD` `Dakar`

---

*© 2025 — Aïcha SAGNE — ESP/UCAD Dakar — Sonatel DSI/EAI/MRI*
