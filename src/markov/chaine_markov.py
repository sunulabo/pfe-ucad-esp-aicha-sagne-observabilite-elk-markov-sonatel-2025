import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger("markov_chain")

ETATS_MARKOV = ["S_In", "S_Proc", "S_Out_OK", "S_Reject", "S_Leak"]

# Normalisation des noms de flux
# msc  -> mobile  (E1_FILTER, pas d'agrégation)
# data -> ggsn    (Filter, agrégation splits)
_FLUX_NORM = {"msc": "mobile", "data": "ggsn"}


class ChaineMarkov:
    """
    Chaîne de Markov à 5 états modélisant un pipeline de traitement de fichiers.

        S_In --> S_Proc --> S_Out_OK   (succès)
                        |-- S_Reject   (rejet explicite)
                        +-- S_Leak     (fuite — calculée par soustraction)

    Propriété fondamentale :
        P(S_Out_OK) + P(S_Reject) + P(S_Leak) = 1
    """

    def __init__(self, idMarkov: str):
        self.idMarkov = idMarkov
        self.Etats: List[str] = ETATS_MARKOV
        self.matriceTransition: Dict[str, Dict[str, Dict[str, float]]] = {}

    def calculerProbabilites(self, df_markov_vols: pd.DataFrame, flux: str = "mobile") -> List[Dict]:
        """
        Calcule les probabilités de transition depuis S_Proc pour chaque filename.

        flux mobile/msc : pas d'agrégation — chaque fichier traité individuellement
        flux ggsn/data  : agrégation des splits (1_xxx, 2_xxx ...) en fichier source
                          résultats par split ET par agrégat
        """
        if df_markov_vols.empty:
            return []

        required = {"workflow_name", "filename", "n_proc", "n_ok", "n_reject"}
        missing = required - set(df_markov_vols.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans df_markov_vols: {sorted(missing)}")

        flux_norm = _FLUX_NORM.get(flux.lower(), flux.lower())
        docs: List[Dict] = []
        self.matriceTransition = {}
        df_markov_vols = df_markov_vols.copy()

        if flux_norm == "ggsn":
            # Fichiers splittés 1_xxx, 2_xxx, 3_xxx, 4_xxx -> agrégation par fichier source
            df_markov_vols["source_file"]  = df_markov_vols["filename"].str.replace(r'^\d+_', '', regex=True)
            df_markov_vols["is_aggregate"] = False

            df_agg = df_markov_vols.groupby("source_file", as_index=False)[["n_proc", "n_ok", "n_reject"]].sum()
            wf_map = df_markov_vols.groupby("source_file")["workflow_name"].first().to_dict()
            df_agg["workflow_name"] = df_agg["source_file"].map(wf_map)
            df_agg["filename"]      = df_agg["source_file"]
            df_agg["is_aggregate"]  = True

            df_combined = pd.concat(
                [df_markov_vols[["workflow_name", "filename", "n_proc", "n_ok", "n_reject", "is_aggregate"]],
                 df_agg[["workflow_name", "filename", "n_proc", "n_ok", "n_reject", "is_aggregate"]]],
                ignore_index=True
            )
        else:
            # MOBILE/MSC : pas de split, pas d'agrégation
            df_markov_vols["is_aggregate"] = False
            df_combined = df_markov_vols[
                ["workflow_name", "filename", "n_proc", "n_ok", "n_reject", "is_aggregate"]
            ].copy()

        for _, row in df_combined.iterrows():
            workflow_name = row["workflow_name"]
            filename      = row["filename"]
            n_proc        = int(row["n_proc"])
            n_ok          = int(row["n_ok"])
            n_reject      = int(row["n_reject"])
            is_aggregate  = bool(row["is_aggregate"])

            if n_proc <= 0:
                logger.warning("[%s] n_proc=0, fichier ignoré", filename)
                continue

            p_ok     = min(1.0, max(0.0, n_ok     / n_proc))
            p_reject = min(1.0, max(0.0, n_reject / n_proc))
            p_leak   = max(0.0, 1.0 - p_ok - p_reject)
            n_leak   = max(0, n_proc - n_ok - n_reject)

            transition_row: Dict[str, float] = {
                "S_In":     0.0,
                "S_Proc":   0.0,
                "S_Out_OK": round(p_ok,     6),
                "S_Reject": round(p_reject, 6),
                "S_Leak":   round(p_leak,   6),
            }

            self.matriceTransition.setdefault(workflow_name, {}).setdefault(filename, {})["S_Proc"] = transition_row

            docs.append({
                "workflow_name":  workflow_name,
                "filename":       filename,
                "etat_source":    "S_Proc",
                "is_aggregate":   is_aggregate,
                "n_proc":         n_proc,
                "n_ok":           n_ok,
                "n_reject":       n_reject,
                "n_leak":         n_leak,
                "kpi_succes":     round(p_ok,     6),
                "kpi_rejet":      round(p_reject, 6),
                "kpi_fuite":      round(p_leak,   6),
                "transition":     transition_row,
                "volumes_destinations": {
                    "S_Proc":   n_proc,
                    "S_Out_OK": n_ok,
                    "S_Reject": n_reject,
                    "S_Leak":   n_leak,
                },
            })

            logger.debug(
                "[%s][%s] n_proc=%d | n_ok=%d P(ok)=%.2f%% | n_reject=%d P(reject)=%.2f%% | n_leak=%d P(leak)=%.2f%%",
                workflow_name, filename, n_proc,
                n_ok,     p_ok     * 100,
                n_reject, p_reject * 100,
                n_leak,   p_leak   * 100,
            )

        return docs

    def detecterDeviation(
        self,
        stats: List[Dict],
        seuil_fuite: float = 0.0,
        baseline: Optional[Dict] = None,
        tolerance: float = 0.05,
    ) -> List[Dict]:
        incidents: List[Dict] = []

        for item in stats:
            filename       = item["filename"]
            etat_source    = item["etat_source"]
            p_ok           = item["kpi_succes"]
            p_reject       = item["kpi_rejet"]
            p_leak         = item["kpi_fuite"]
            n_proc         = item["n_proc"]
            n_leak         = item["n_leak"]
            somme_observee = p_ok + p_reject

            if p_leak > seuil_fuite:
                incidents.append({
                    "type":           "LEAKAGE",
                    "filename":       filename,
                    "etat_source":    etat_source,
                    "kpi_fuite":      round(p_leak, 6),
                    "n_leak":         n_leak,
                    "n_proc":         n_proc,
                    "somme_observee": round(somme_observee, 6),
                    "message": (
                        f"[{filename}] Fuite depuis {etat_source} : "
                        f"P(S_Leak)={p_leak:.2%} ({n_leak}/{n_proc} UDRs disparus). "
                        f"Somme probabilités observées = {somme_observee:.4f} != 1"
                    ),
                })

            if not baseline:
                continue

            ref = baseline.get(filename, {}).get(etat_source)
            if not ref:
                continue

            for metric, current_val in [
                ("kpi_succes", p_ok),
                ("kpi_rejet",  p_reject),
                ("kpi_fuite",  p_leak),
            ]:
                ref_val = ref.get(metric)
                if ref_val is None:
                    continue
                ecart = abs(current_val - ref_val)
                if ecart > tolerance:
                    incidents.append({
                        "type":        "DEVIATION",
                        "filename":    filename,
                        "etat_source": etat_source,
                        "metric":      metric,
                        "current":     round(current_val, 6),
                        "baseline":    round(ref_val,     6),
                        "ecart":       round(ecart,       6),
                        "message": (
                            f"[{filename}] Déviation sur {metric} depuis {etat_source} : "
                            f"courant={current_val:.4f}, baseline={ref_val:.4f}, "
                            f"écart={ecart:.4f} > tolérance={tolerance:.4f}"
                        ),
                    })

        return incidents

    def getMatriceTransition(self, filename: Optional[str] = None) -> Dict:
        if filename:
            return self.matriceTransition.get(filename, {})
        return self.matriceTransition

    def __repr__(self) -> str:
        return (
            f"ChaineMarkov(id={self.idMarkov!r}, "
            f"etats={self.Etats}, "
            f"fichiers_calcules={list(self.matriceTransition.keys())})"
        )