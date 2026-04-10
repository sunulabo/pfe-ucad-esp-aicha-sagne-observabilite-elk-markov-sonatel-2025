import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("connexion_bd")

DEFAULT_DB_CONFIG_PATH = "db_config.json"
DEFAULT_MAPPING_PATH   = "config_mapping.json"


def _load_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_db_entry(
    entry: Optional[Dict] = None,
    config_path: str = DEFAULT_DB_CONFIG_PATH,
    db_name: Optional[str] = None,
) -> Dict:
    if entry is not None:
        return entry
    config = _load_json(config_path)
    databases = config.get("databases", [])
    if not databases:
        raise ValueError(f"Aucune base configuree dans {config_path}")
    if db_name:
        selected = next((db for db in databases if db.get("name") == db_name), None)
        if selected is None:
            raise ValueError(f"Base '{db_name}' introuvable dans {config_path}")
        return selected
    return databases[0]


def get_db_conn(
    entry: Dict = None,
    config_path: str = DEFAULT_DB_CONFIG_PATH,
    db_name: str = None,
):
    """Connexion MySQL/MariaDB via mysql-connector-python."""
    db_entry = _resolve_db_entry(entry=entry, config_path=config_path, db_name=db_name)
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=db_entry.get("host", "localhost"),
            port=int(db_entry.get("port", 3306)),
            database=db_entry.get("dbname", "audit_markov"),
            user=db_entry.get("user", "root"),
            password=db_entry.get("password", ""),
        )
        return conn
    except ImportError:
        raise ImportError(
            "mysql-connector-python non installé.\n"
            "Lancez : pip install mysql-connector-python"
        )


def load_node_mapping(mapping_path: str = DEFAULT_MAPPING_PATH) -> Dict[str, str]:
    return _load_json(mapping_path).get("nodes", {})


def _build_in_clause(nodes: List[str]) -> str:
    """Construit IN ('val1', 'val2', ...) compatible MySQL/MariaDB."""
    escaped = ", ".join(f"'{n}'" for n in nodes)
    return f"({escaped})"


def _table_exists(conn, table_name: str) -> bool:
    """Vérifie si une table existe dans la base."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table_name,)
    )
    exists = cur.fetchone()[0] > 0
    cur.close()
    return exists


def _extract_collect(conn, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """
    Extraction depuis audit_markov_event_collect.
    Retourne DataFrame vide si la table n'existe pas.
    """
    if not _table_exists(conn, "audit_markov_event_collect"):
        logger.info("Table 'audit_markov_event_collect' absente — collect ignoré.")
        return pd.DataFrame(
            columns=["flux_name", "udr_key", "source_node", "dest_node", "volume"]
        )

    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
            workflow_name                        AS flux_name,
            COALESCE(udr_key, 'UNKNOWN')         AS udr_key,
            source                               AS source_node,
            destination                          AS dest_node,
            COUNT(*)                             AS volume
        FROM audit_markov_event_collect
        WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
        GROUP BY workflow_name, udr_key, source, destination
        """,
        (start_time, end_time),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return pd.DataFrame(
            columns=["flux_name", "udr_key", "source_node", "dest_node", "volume"]
        )
    return pd.DataFrame(rows)


def _compute_markov_volumes(
    conn,
    start_time: datetime,
    end_time: datetime,
    node_mapping: Dict[str, str],
    source_filter: str = "E1_FILTER",
    reject_from_proc: bool = False,
) -> pd.DataFrame:
    """
    Calcule les volumes bruts par filename.

    MODE GGSN (source_filter='Filter', reject_from_proc=True) :
      Table    : audit_markov_event_proc_ggsn
      n_proc   = COUNT(DISTINCT udr_key) WHERE source = 'Filter'
      n_ok     = COUNT(DISTINCT udr_key) WHERE destination ∈ S_Out_OK
      n_reject = COUNT(DISTINCT udr_key) WHERE destination ∈ S_Reject

    MODE MOBILE (source_filter='E1_FILTER', reject_from_proc=False) :
      Tables   : audit_markov_event_preproc + audit_markov_event_proc
      Fallback : si ces tables manquent → utilise audit_markov_event_proc_ggsn

    Retourne : workflow_name | filename | n_proc | n_ok | n_reject
    """
    ok_nodes     = [n for n, s in node_mapping.items() if s == "S_Out_OK"]
    reject_nodes = [n for n, s in node_mapping.items() if s == "S_Reject"]
    ok_in        = _build_in_clause(ok_nodes)
    rej_in       = _build_in_clause(reject_nodes)

    cur = conn.cursor(dictionary=True)

    if reject_from_proc:
        # ── MODE GGSN ──────────────────────────────────────
        table = "audit_markov_event_proc_ggsn"

        if not _table_exists(conn, table):
            logger.warning(f"Table '{table}' introuvable.")
            cur.close()
            return pd.DataFrame(
                columns=["workflow_name", "filename", "n_proc", "n_ok", "n_reject"]
            )

        # n_proc : UDRs distincts entrant via Filter
        q_in = f"""
            SELECT
                workflow_name,
                COALESCE(filename, udr_key, 'UNKNOWN') AS filename,
                COUNT(DISTINCT udr_key)                AS n_proc
            FROM {table}
            WHERE source = %s
              AND COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
            GROUP BY workflow_name, COALESCE(filename, udr_key, 'UNKNOWN')
        """
        cur.execute(q_in, (source_filter, start_time, end_time))
        rows_in = cur.fetchall()
        df_in = (
            pd.DataFrame(rows_in)
            if rows_in else
            pd.DataFrame(columns=["workflow_name", "filename", "n_proc"])
        )

        # n_ok + n_reject : UDRs distincts sortant
        q_out = f"""
            SELECT
                workflow_name,
                COALESCE(filename, udr_key, 'UNKNOWN')              AS filename,
                COUNT(DISTINCT CASE WHEN destination IN {ok_in}
                               THEN udr_key END)                    AS n_ok,
                COUNT(DISTINCT CASE WHEN destination IN {rej_in}
                               THEN udr_key END)                    AS n_reject
            FROM {table}
            WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
            GROUP BY workflow_name, COALESCE(filename, udr_key, 'UNKNOWN')
        """
        cur.execute(q_out, (start_time, end_time))
        rows_out = cur.fetchall()
        df_out = (
            pd.DataFrame(rows_out)
            if rows_out else
            pd.DataFrame(columns=["workflow_name", "filename", "n_ok", "n_reject"])
        )
        cur.close()

        if df_in.empty:
            return pd.DataFrame(
                columns=["workflow_name", "filename", "n_proc", "n_ok", "n_reject"]
            )

        result = pd.merge(
            df_in, df_out,
            on=["workflow_name", "filename"],
            how="left"
        ).fillna(0)

    else:
        # ── MODE MOBILE ────────────────────────────────────
        # Vérifier si les tables Mobile existent
        has_preproc = _table_exists(conn, "audit_markov_event_preproc")
        has_proc    = _table_exists(conn, "audit_markov_event_proc")

        if not has_preproc and not has_proc:
            # Fallback : utiliser audit_markov_event_proc_ggsn si disponible
            logger.warning(
                "Tables Mobile introuvables — "
                "utilisation de audit_markov_event_proc_ggsn en fallback."
            )
            cur.close()
            return _compute_markov_volumes(
                conn, start_time, end_time, node_mapping,
                source_filter="Filter", reject_from_proc=True
            )

        if has_preproc:
            # n_proc + n_reject depuis preproc
            q_preproc = f"""
                SELECT
                    COALESCE(filename, udr_key, 'UNKNOWN')              AS filename,
                    COUNT(*)                                            AS n_proc,
                    COUNT(CASE WHEN destination IN {rej_in} THEN 1 END) AS n_reject
                FROM audit_markov_event_preproc
                WHERE source = %s
                  AND COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
                GROUP BY COALESCE(filename, udr_key, 'UNKNOWN')
            """
            cur.execute(q_preproc, (source_filter, start_time, end_time))
            rows_preproc = cur.fetchall()
            df_preproc = (
                pd.DataFrame(rows_preproc)
                if rows_preproc else
                pd.DataFrame(columns=["filename", "n_proc", "n_reject"])
            )
        else:
            df_preproc = pd.DataFrame(columns=["filename", "n_proc", "n_reject"])

        if has_proc:
            # workflow_name + n_ok depuis proc
            q_proc = f"""
                SELECT
                    workflow_name,
                    COALESCE(filename, udr_key, 'UNKNOWN')              AS filename,
                    COUNT(CASE WHEN destination IN {ok_in} THEN 1 END)  AS n_ok
                FROM audit_markov_event_proc
                WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
                GROUP BY workflow_name, COALESCE(filename, udr_key, 'UNKNOWN')
            """
            cur.execute(q_proc, (start_time, end_time))
            rows_proc = cur.fetchall()
            df_proc = (
                pd.DataFrame(rows_proc)
                if rows_proc else
                pd.DataFrame(columns=["workflow_name", "filename", "n_ok"])
            )
        else:
            df_proc = pd.DataFrame(columns=["workflow_name", "filename", "n_ok"])

        cur.close()

        if df_preproc.empty:
            return pd.DataFrame(
                columns=["workflow_name", "filename", "n_proc", "n_ok", "n_reject"]
            )

        result = pd.merge(df_preproc, df_proc, on="filename", how="left").fillna(0)

    result["n_proc"]        = result["n_proc"].astype(int)
    result["n_ok"]          = result["n_ok"].astype(int)
    result["n_reject"]      = result["n_reject"].astype(int)
    result["workflow_name"] = result["workflow_name"].astype(str)

    return result[["workflow_name", "filename", "n_proc", "n_ok", "n_reject"]]


def _compute_agent_audit_v2(
    conn,
    start_time: datetime,
    end_time: datetime,
    table: str = "audit_markov_event_proc_ggsn"
) -> pd.DataFrame:
    """
    Calcule n_in, n_out et n_leak_internal par AGENT.
    Utilise REGEXP_REPLACE pour normaliser l'UDR_KEY.
    """
    if not _table_exists(conn, table):
        logger.warning(f"Table '{table}' absente, abandon de l'audit agent.")
        return pd.DataFrame()

    cur = conn.cursor(dictionary=True)
    
    # Nettoyage de la clé : retire tout après le dernier tiret (ex: -Filter)
    sql_clean_key = "REGEXP_REPLACE(udr_key, '-[^-]+$', '')"

    query = f"""
        SELECT 
            workflow_name,
            filename,
            agent_name,
            COUNT(DISTINCT base_key) as udr_count,
            direction
        FROM (
            SELECT 
                workflow_name,
                filename, 
                destination as agent_name, 
                {sql_clean_key} as base_key, 
                'IN' as direction
            FROM {table} 
            WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
            UNION ALL
            SELECT 
                workflow_name,
                filename, 
                source as agent_name, 
                {sql_clean_key} as base_key, 
                'OUT' as direction
            FROM {table} 
            WHERE COALESCE(process_end_time, process_start_time) BETWEEN %s AND %s
        ) as flow
        GROUP BY workflow_name, filename, agent_name, direction
    """
    
    try:
        cur.execute(query, (start_time, end_time, start_time, end_time))
        rows = cur.fetchall()
        cur.close()
        
        if not rows:
            return pd.DataFrame()

        df_raw = pd.DataFrame(rows)
        # Pivot pour mettre IN et OUT en colonnes
        df_pivot = df_raw.pivot_table(
            index=['workflow_name', 'filename', 'agent_name'], 
            columns='direction', 
            values='udr_count', 
            fill_value=0
        ).reset_index()
        
        df_pivot.columns.name = None
        # Sécurité : s'assurer que les colonnes existent
        if 'IN' not in df_pivot.columns: df_pivot['IN'] = 0
        if 'OUT' not in df_pivot.columns: df_pivot['OUT'] = 0

        # Calcul de la fuite interne
        df_pivot['n_leak_internal'] = (df_pivot['IN'] - df_pivot['OUT']).clip(lower=0)
        
        # Calcul du taux de perte interne (%)
        df_pivot['p_leak_internal'] = (df_pivot['n_leak_internal'] / df_pivot['IN'].replace(0, 1)).fillna(0)
        
        return df_pivot
    except Exception as e:
        logger.error(f"Erreur lors de l'audit agent v2 : {e}")
        return pd.DataFrame()


def _fetch_from_db(
    db_entry: Dict,
    start_time: datetime,
    end_time: datetime,
    node_mapping: Dict[str, str],
    source_filter: str = "E1_FILTER",
    reject_from_proc: bool = False,
) -> Dict:
    conn = None
    try:
        conn = get_db_conn(db_entry)
        df_collect     = _extract_collect(conn, start_time, end_time)
        df_markov_vols = _compute_markov_volumes(
            conn, start_time, end_time, node_mapping,
            source_filter=source_filter,
            reject_from_proc=reject_from_proc,
        )
        
        # En fonction du flux on cible la bonne table (GGSN par défaut, ou PROC pour mobile)
        table_agent = "audit_markov_event_proc_ggsn" if reject_from_proc else "audit_markov_event_proc"
        df_agents = _compute_agent_audit_v2(conn, start_time, end_time, table=table_agent)
        
        return {"df_collect": df_collect, "df_markov_vols": df_markov_vols, "df_agents": df_agents}
    except Exception as exc:
        logger.error("Erreur extraction sur %s: %s", db_entry.get("name", "db"), exc)
        return {"df_collect": pd.DataFrame(), "df_markov_vols": pd.DataFrame(), "df_agents": pd.DataFrame()}
    finally:
        if conn is not None:
            conn.close()


def fetch_recent_traces(
    db_config: Dict,
    mapping_path: str = DEFAULT_MAPPING_PATH,
    window_minutes: int = 5,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    source_filter: str = "E1_FILTER",
    reject_from_proc: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Collecte en parallèle depuis toutes les bases configurées.

    Retourne (df_collect, df_markov_vols, df_agents) :
      df_collect     -- flux_name | udr_key | source_node | dest_node | volume
      df_markov_vols -- workflow_name | filename | n_proc | n_ok | n_reject
      df_agents      -- filename | agent_name | IN | OUT | n_leak_internal | p_leak_internal
    """
    databases = db_config.get("databases", [])
    if not databases:
        logger.warning("Aucune base configurée dans db_config")
        return pd.DataFrame(), pd.DataFrame()

    if start_time is None and end_time is None:
        end_time   = datetime.utcnow()
        start_time = end_time - timedelta(minutes=window_minutes)
    elif start_time is None:
        start_time = end_time - timedelta(minutes=window_minutes)
    elif end_time is None:
        end_time = start_time + timedelta(minutes=window_minutes)

    node_mapping = load_node_mapping(mapping_path)

    collect_parts:    List[pd.DataFrame] = []
    markov_vol_parts: List[pd.DataFrame] = []
    agent_parts:      List[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(databases)))) as pool:
        futures = [
            pool.submit(
                _fetch_from_db, db, start_time, end_time, node_mapping,
                source_filter, reject_from_proc,
            )
            for db in databases
        ]
        for future in futures:
            result = future.result()
            if not result["df_collect"].empty:
                collect_parts.append(result["df_collect"])
            if not result["df_markov_vols"].empty:
                markov_vol_parts.append(result["df_markov_vols"])
            if not result["df_agents"].empty:
                agent_parts.append(result["df_agents"])

    df_collect = (
        pd.concat(collect_parts, ignore_index=True)
        .groupby(
            ["flux_name", "udr_key", "source_node", "dest_node"],
            as_index=False
        )["volume"].sum()
        if collect_parts else pd.DataFrame()
    )

    df_markov_vols = (
        pd.concat(markov_vol_parts, ignore_index=True)
        .groupby(
            ["workflow_name", "filename"],
            as_index=False
        )[["n_proc", "n_ok", "n_reject"]].sum()
        if markov_vol_parts else pd.DataFrame()
    )
    
    df_agents = (
        pd.concat(agent_parts, ignore_index=True)
        .groupby(
            ["workflow_name", "filename", "agent_name"],
            as_index=False
        )[["IN", "OUT", "n_leak_internal"]].sum()
        if agent_parts else pd.DataFrame()
    )
    if not df_agents.empty:
        df_agents['p_leak_internal'] = (df_agents['n_leak_internal'] / df_agents['IN'].replace(0, 1)).fillna(0)

    return df_collect, df_markov_vols, df_agents