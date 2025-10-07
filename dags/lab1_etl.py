# dag_yfinance_per_ticker_tables.py
# Airflow 2.x, Python 3.10+
# Requirements:
#   yfinance>=0.2.40
#   pandas>=2.1
#   snowflake-connector-python>=3.8.0

from __future__ import annotations
from datetime import timedelta, date
import json, logging, re
from typing import List, Dict, Tuple, DefaultDict
from collections import defaultdict

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------- Constants & Defaults -----------------------------

DEFAULT_ARGS = {
    "owner": "Dhruv-Drashti",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SNOWFLAKE_CONN_ID = "snowflake_conn"
DEFAULT_TICKERS = ["AAPL", "NVDA"]
DEFAULT_LOOKBACK_DAYS = 180
DEFAULT_SCHEMA = "RAW"


# ----------------------------- Helper Functions -----------------------------

def return_snowflake_conn():
    """Return Snowflake connection."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


def _get_list_variable(key: str, default_list: List[str]) -> List[str]:
    try:
        raw = Variable.get(key, default_var=json.dumps(default_list))
        val = json.loads(raw) if isinstance(raw, str) else raw
        return val if isinstance(val, list) else default_list
    except Exception:
        return default_list


def _get_int_variable(key: str, default_int: int) -> int:
    try:
        return int(Variable.get(key, default_var=str(default_int)))
    except Exception:
        return default_int


def _get_str_variable(key: str, default_str: str) -> str:
    try:
        return str(Variable.get(key, default_var=default_str))
    except Exception:
        return default_str


def _quote_ident(ident: str) -> str:
    """Safely quote Snowflake identifiers (e.g. BRK.B -> "BRK.B")."""
    return f"\"{ident.replace('\"', '\"\"')}\""


def table_fqn(schema: str, symbol: str) -> str:
    """Fully qualified table name like RAW."AAPL"."""
    return f"{schema}.{_quote_ident(symbol)}"


def ensure_schema_and_table(conn, schema: str, symbol: str) -> None:
    """Ensure schema and table for ticker exist."""
    tbl = table_fqn(schema, symbol)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
            SYMBOL STRING NOT NULL,
            DT DATE NOT NULL,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME NUMBER(38,0),
            CONSTRAINT PK_{re.sub(r'[^A-Za-z0-9_]', '_', symbol)} PRIMARY KEY (SYMBOL, DT)
        )
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(ddl)


# ----------------------------- ETL Tasks -----------------------------

@task
def resolve_config() -> Dict:
    """Resolve runtime config from Airflow Variables."""
    logger = logging.getLogger("airflow.task")
    tickers = _get_list_variable("yf_tickers", DEFAULT_TICKERS)
    lookback_days = _get_int_variable("yf_lookback_days", DEFAULT_LOOKBACK_DAYS)
    schema = _get_str_variable("yf_target_schema", DEFAULT_SCHEMA)
    end = date.today()
    start = end - timedelta(days=lookback_days)
    cfg = {"tickers": [t.strip().upper() for t in tickers], "start": str(start), "end": str(end), "schema": schema}
    logger.info("Resolved config: %s", cfg)
    return cfg


@task
def extract(cfg: Dict) -> List[Dict]:
    """
    Extract OHLCV daily bars from yfinance for the configured tickers and window.
    Returns JSON-safe list[dict] (all keys are strings; dates are YYYY-MM-DD strings).
    """
    logger = logging.getLogger("airflow.task")
    out_rows: List[Dict] = []

    for t in cfg["tickers"]:
        df = yf.download(
            t,
            start=cfg["start"],
            end=cfg["end"],
            auto_adjust=False,
            progress=False,
            interval="1d",
        )

        if df is None or df.empty:
            logger.warning("No data returned for %s", t)
            continue

        # 1) Flatten possible MultiIndex columns (e.g., ('Close','AAPL') -> 'Close')
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        # 2) Uniform upper-case column names
        df.columns = [str(c).upper() for c in df.columns]

        # 3) Build a tidy frame with JSON-safe values
        df = df.reset_index().rename(
            columns={
                "DATE": "DT",
                "OPEN": "OPEN",
                "HIGH": "HIGH",
                "LOW": "LOW",
                "CLOSE": "CLOSE",
                "VOLUME": "VOLUME",
            }
        )

        # Some yfinance versions label the index column differently after reset_index()
        if "DT" not in df.columns:
            # Fallback: first column after reset_index is the datetime
            dt_col = df.columns[0]
            df = df.rename(columns={dt_col: "DT"})

        # Stringify date to avoid JSON serialization issues
        df["DT"] = pd.to_datetime(df["DT"]).dt.strftime("%Y-%m-%d")
        df["SYMBOL"] = t

        wanted = ["DT", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL"]
        missing = [c for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"] if c not in df.columns]
        if missing:
            logger.error("Missing expected columns for %s: %s", t, missing)
            continue

        recs = df[wanted].to_dict(orient="records")
        out_rows.extend(recs)
        logger.info("Extracted %d rows for %s", len(recs), t)

    logger.info("✅ Total extracted rows: %d", len(out_rows))
    return out_rows


@task
def transform(raw_records: List[Dict]) -> List[Tuple]:
    """
    Casts types, normalizes keys, and returns rows as tuples for executemany.
    Schema/order: SYMBOL, DT (YYYY-MM-DD string), OPEN, HIGH, LOW, CLOSE, VOLUME
    """
    logger = logging.getLogger("airflow.task")
    if not raw_records:
        logger.warning("No records received for transform.")
        return []

    df = pd.DataFrame(raw_records)

    # Enforce numeric types; DT stays as string for JSON/XCom safety
    df = df.astype(
        {
            "OPEN": float,
            "HIGH": float,
            "LOW": float,
            "CLOSE": float,
            "VOLUME": "int64",
            "SYMBOL": "string",
            "DT": "string",
        }
    )

    ordered = df[["SYMBOL", "DT", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]
    tuples: List[Tuple] = list(ordered.itertuples(index=False, name=None))
    logger.info("✅ Transformed rows: %d", len(tuples))
    return tuples


@task
def load(cfg: Dict, tuples: List[Tuple]) -> int:
    """
    Phase A (DDL): ensure schema and per-ticker table exist, committed independently.
    Phase B (DML): per-ticker upsert using a TEMP table inside a transaction.
    """
    logger = logging.getLogger("airflow.task")
    if not tuples:
        logger.warning("No rows to load.")
        return 0

    # Group rows by symbol
    grouped: DefaultDict[str, List[Tuple]] = defaultdict(list)
    for rec in tuples:
        grouped[rec[0]].append(rec)

    # ---------------- Phase A: DDL (autocommitted / explicitly committed) ----------------
    ddl_conn = return_snowflake_conn()
    try:
        with ddl_conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg['schema']}")
            for symbol in grouped.keys():
                ensure_schema_and_table(ddl_conn, cfg["schema"], symbol)
        ddl_conn.commit()
        logger.info("✅ DDL ensured for %d tables in schema %s", len(grouped), cfg["schema"])
    except Exception as e:
        ddl_conn.rollback()
        logger.error("❌ DDL phase failed: %s", e, exc_info=True)
        raise
    finally:
        ddl_conn.close()

    # ---------------- Phase B: DML (transactional upsert) ----------------
    conn = return_snowflake_conn()
    total = 0
    try:
        with conn.cursor() as cur:
            # Ensure this session has a current schema to allow temp tables
            cur.execute(f"USE SCHEMA {cfg['schema']}")

            for symbol, rows in grouped.items():
                tbl = table_fqn(cfg["schema"], symbol)

                # Safe temp table name: schema-qualified, unique per symbol
                temp_name = f"T_LOAD_{re.sub(r'[^A-Za-z0-9_]', '_', symbol)}"
                temp_tbl = f"{cfg['schema']}.{temp_name}"

                # Create empty temp table matching target structure
                cur.execute(f"CREATE OR REPLACE TEMP TABLE {temp_tbl} AS SELECT * FROM {tbl} WHERE 1=0")

                # Insert rows into temp table (cast DT to DATE)
                insert_sql = f"""
                    INSERT INTO {temp_tbl} (SYMBOL, DT, OPEN, HIGH, LOW, CLOSE, VOLUME)
                    VALUES (%s, TO_DATE(%s), %s, %s, %s, %s, %s)
                """
                CHUNK = 10_000
                for i in range(0, len(rows), CHUNK):
                    cur.executemany(insert_sql, rows[i:i+CHUNK])

                # Merge from temp into main table
                cur.execute(f"""
                    MERGE INTO {tbl} t
                    USING {temp_tbl} s
                    ON t.SYMBOL = s.SYMBOL AND t.DT = s.DT
                    WHEN MATCHED THEN UPDATE SET
                        OPEN = s.OPEN,
                        HIGH = s.HIGH,
                        LOW  = s.LOW,
                        CLOSE = s.CLOSE,
                        VOLUME = s.VOLUME
                    WHEN NOT MATCHED THEN INSERT (SYMBOL, DT, OPEN, HIGH, LOW, CLOSE, VOLUME)
                    VALUES (s.SYMBOL, s.DT, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME)
                """)

                total += len(rows)

        conn.commit()
        logger.info("✅ Loaded %d rows across %d tables", total, len(grouped))
        return total

    except Exception as e:
        conn.rollback()
        logger.error("❌ DML phase failed; rolled back: %s", e, exc_info=True)
        raise
    finally:
        conn.close()

# ----------------------------- DAG Definition -----------------------------

with DAG(
    dag_id="yfinance_etl",
    description="ETL: yfinance OHLCV → Snowflake",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="0 6 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ETL", "yfinance", "snowflake"],
) as dag:

    cfg = resolve_config()
    raw = extract(cfg)
    shaped = transform(raw)
    loaded = load(cfg, shaped)

    trigger_train_forecast = TriggerDagRunOperator(
        task_id="trigger_train_forecast",
        trigger_dag_id="snowflake_train_and_forecast_parallel",
        wait_for_completion=True,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        reset_dag_run=True,
        poke_interval=60,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # explicit (default)
    )

    loaded >> trigger_train_forecast
