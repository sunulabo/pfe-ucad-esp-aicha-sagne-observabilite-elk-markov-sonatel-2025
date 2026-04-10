# Projet Markov Audit

Pipeline d'observabilite pour calculer des KPI Markov sur les traces d'audit.

## Installation

```bash
pip install -r requirements.txt
```

## Variables d'environnement

- `MARKOV_WINDOW_MINUTES` (defaut: `5`)
- `MARKOV_LEAK_THRESHOLD` (defaut: `0.0`)
- `MARKOV_DEVIATION_TOLERANCE` (defaut: `0.05`)
- `MARKOV_BASELINE_PATH` (optionnel)
- `ELASTIC_HOSTS` (ex: `http://localhost:9200`)
- `ELASTIC_USER` / `ELASTIC_PASSWORD` (optionnel)
- `ELASTIC_INDEX` (defaut: `flux-markov-stats`)

## Lancement

```bash
python main.py
```

## Extraction SQL

- Les tables lues sont celles declarees dans `db_config.json`.
- `audit_markov_event_collect` est groupee avec `udr_key` comme `filename`.
- `audit_markov_event_preproc` et `audit_markov_event_proc` sont jointes sur `filename`.
- Le groupage final est: `(flux_name, filename, source_node, dest_node)`.

## Logique de calcul

Pour chaque `(flux, filename, etat_source)` :

- `P(S_Out_OK | etat_source) = volume_vers_S_Out_OK / total_out`
- `P(S_Reject | etat_source) = volume_vers_S_Reject / total_out`
- `P(S_Proc | etat_source) = volume_vers_S_Proc / total_out`
- Si `etat_source = S_Proc`:
  - `P(S_Leak | S_Proc) = 1 - (P(S_Out_OK | S_Proc) + P(S_Reject | S_Proc))`
- Sinon:
  - `P(S_Leak | etat_source) = 1 - (P(S_Proc | etat_source) + P(S_Out_OK | etat_source) + P(S_Reject | etat_source))`

Une alerte est levee si `kpi_fuite > MARKOV_LEAK_THRESHOLD`.
