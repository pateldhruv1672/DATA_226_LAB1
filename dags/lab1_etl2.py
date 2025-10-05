# dag_common_features_per_ticker.py
# Airflow 2.x, Python 3.10+
# Purpose:
#   Build a *separate Snowflake table per ticker* with model-ready features for
#   forecasting + sentiment analysis (price OHLCV, returns/lags, MAs/STD, RSI,
#   volume features, and joined daily sentiment with short rolling windows).
#
# Upstream expectations:
#   - RAW schema already has per-ticker tables from your yfinance DAG (e.g., RAW."AAPL")
#   - NEWS_SENTIMENT_DAILY exists with columns: SYMBOL, DT, SENT_MEAN, SENT_COUNT
#
# Requirements:
#   snowflake-connector-python>=3.8.0
#   pandas>=2.1  (not required for SQL path, but useful if you later extend)
#
# Airflow Variables:
#   "yf_tickers"             -> e.g. '["AAPL","NVDA","MSFT"]'
#   "yf_target_schema"       -> e.g. "RAW"            (where per-ticker price tables live)
#   "features_target_schema" -> e.g. "FEATURES"       (where per-ticker FEATURE tables go)
#   "news_sentiment_daily"   -> e.g. "NEWS_SENTIMENT_DAILY"
#   "feature_lookback_days"  -> e.g. "365"
#
from __future__ import annotations
from datetime import timedelta, date
import json, logging, re
from typing import List, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# ----------------------------- Constants & Defaults -----------------------------

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SNOWFLAKE_CONN_ID = "snowflake_conn"
DEFAULT_TICKERS = ["AAPL", "NVDA"]
DEFAULT_RAW_SCHEMA = "RAW"
DEFAULT_FEATURES_SCHEMA = "FEATURES"
DEFAULT_SENT_DAILY = "NEWS_SENTIMENT_DAILY"
DEFAULT_LOOKBACK_DAYS = 365

# Feature windows (tweak as needed)
MA_WINDOWS = [3, 7, 14, 30]
STD_WINDOWS = [7, 30]
RSI_PERIOD = 14
VOL_MA = 7
VOL_Z = 30

# ----------------------------- Helper Functions -----------------------------

def return_snowflake_conn():
    """Return Snowflake connection."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

def _get_list_variable(key: str, default_list: List[str]) -> List[str]:
    try:
        raw = Variable.get(key, default_var=json.dumps(default_list))
        val = json.loads(raw) if isinstance(raw, str) else raw
        return [str(x).strip().upper() for x in (val if isinstance(val, list) else default_list)]
    except Exception:
        return default_list

def _get_int_variable(key: str, default_int: int) -> int:
    try:
        return int(Variable.get(key, default_var=str(default_int)))
    except Exception:
        return default_int

def _get_str_variable(key: str, default_str: str) -> str:
    try:
        return str(Variable.get(key, default_var=default_str)).strip()
    except Exception:
        return default_str

def _quote_ident(ident: str) -> str:
    """Safely quote Snowflake identifiers (e.g. BRK.B -> "BRK.B")."""
    return f"\"{ident.replace('\"', '\"\"')}\""

def table_fqn(schema: str, name: str) -> str:
    return f"{schema}.{_quote_ident(name)}"

def ensure_features_table(conn, schema: str, symbol: str) -> None:
    """
    Ensure the per-ticker FEATURES table exists with stable columns.
    If you add columns later, MERGE will still update/insert those that exist.
    """
    tbl = table_fqn(schema, symbol)
    pk = f"PK_{re.sub(r'[^A-Za-z0-9_]', '_', symbol)}_FEAT"
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
            SYMBOL STRING NOT NULL,
            DT DATE NOT NULL,

            -- Raw OHLCV
            OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT, VOLUME NUMBER(38,0),

            -- Lags & returns
            LAG_CLOSE_1 FLOAT, LAG_CLOSE_5 FLOAT,
            RET_1D FLOAT, RET_5D FLOAT,

            -- Rolling stats
            MA_3 FLOAT, MA_7 FLOAT, MA_14 FLOAT, MA_30 FLOAT,
            STD_7 FLOAT, STD_30 FLOAT,

            -- Volume features
            VOL_MA_7 FLOAT,
            VOL_M_30 FLOAT,   -- rolling mean 30
            VOL_S_30 FLOAT,   -- rolling std 30
            VOL_Z_30 FLOAT,   -- z-score

            -- Momentum
            RSI_14 FLOAT,

            -- Sentiment features
            SENT_MEAN FLOAT, SENT_COUNT NUMBER(38,0),
            SENT_MEAN_3D FLOAT, SENT_MEAN_7D FLOAT,

            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

            CONSTRAINT {pk} PRIMARY KEY (SYMBOL, DT)
        )
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(ddl)

def _clean_sym(sym: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", sym).upper()

# ----------------------------- ETL Tasks -----------------------------

@task
def resolve_config() -> Dict:
    """Resolve runtime config from Airflow Variables."""
    logger = logging.getLogger("airflow.task")
    symbols = _get_list_variable("yf_tickers", DEFAULT_TICKERS)
    raw_schema = _get_str_variable("yf_target_schema", DEFAULT_RAW_SCHEMA)
    feat_schema = _get_str_variable("features_target_schema", DEFAULT_FEATURES_SCHEMA)
    sent_daily = _get_str_variable("news_sentiment_daily", DEFAULT_SENT_DAILY)
    lookback_days = _get_int_variable("feature_lookback_days", DEFAULT_LOOKBACK_DAYS)

    # Add buffer for longest rolling window so edges compute correctly
    buffer_days = max(max(MA_WINDOWS), max(STD_WINDOWS + [RSI_PERIOD, VOL_Z])) + 5
    end = date.today()
    start = end - timedelta(days=lookback_days + buffer_days)

    cfg = {
        "symbols": symbols,
        "raw_schema": raw_schema,
        "features_schema": feat_schema,
        "sent_daily": sent_daily,
        "lookback_days": lookback_days,
        "start": str(start),
        "end": str(end),
    }
    logger.info("Resolved config: %s", cfg)
    return cfg

@task
def build_features(cfg: Dict) -> int:
    """
    For each symbol:
      Phase A (DDL): ensure FEATURES schema and per-ticker table exist.
      Phase B (SQL DML): compute features in TEMP staging (SQL) and MERGE into target.
    Returns total rows touched (approx. rows considered across symbols).
    """
    logger = logging.getLogger("airflow.task")
    symbols: List[str] = cfg["symbols"]
    raw_schema = cfg["raw_schema"]
    feat_schema = cfg["features_schema"]
    sent_daily = cfg["sent_daily"]
    start = cfg["start"]

    # ----- Phase A: DDL -----
    ddl_conn = return_snowflake_conn()
    try:
        with ddl_conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {feat_schema}")
            for sym in symbols:
                ensure_features_table(ddl_conn, feat_schema, sym)
        ddl_conn.commit()
        logger.info("✅ DDL ensured for %d feature tables in schema %s", len(symbols), feat_schema)
    except Exception as e:
        ddl_conn.rollback()
        logger.error("❌ DDL phase failed: %s", e, exc_info=True)
        raise
    finally:
        ddl_conn.close()

    # ----- Phase B: DML (transactional upsert via MERGE) -----
    total_rows = 0
    conn = return_snowflake_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE SCHEMA {feat_schema}")

            for sym in symbols:
                raw_tbl = table_fqn(raw_schema, sym)           # e.g., RAW."AAPL"
                feat_tbl = table_fqn(feat_schema, sym)         # e.g., FEATURES."AAPL"
                temp_tbl = f"{feat_schema}.T_STG_FEATURES_{_clean_sym(sym)}"

                # Build SQL for staging features via CTEs. All computed server-side in Snowflake.
                # NOTE: RSI_14 uses simple rolling averages of gains/losses over window.
                sql_staging = f"""
                CREATE OR REPLACE TEMP TABLE {temp_tbl} AS
                WITH BASE AS (
                  SELECT DT, OPEN, HIGH, LOW, CLOSE, VOLUME
                  FROM {raw_tbl}
                  WHERE DT >= '{start}'
                ),
                SENT AS (
                  SELECT DT, SENT_MEAN, SENT_COUNT
                  FROM {sent_daily}
                  WHERE SYMBOL = '{sym}' AND DT >= '{start}'
                ),
                J AS (
                  SELECT
                    b.DT, b.OPEN, b.HIGH, b.LOW, b.CLOSE, b.VOLUME,
                    s.SENT_MEAN, s.SENT_COUNT
                  FROM BASE b
                  LEFT JOIN SENT s USING (DT)
                ),
                F AS (
                  SELECT
                    '{sym}'::STRING AS SYMBOL,
                    DT,
                    OPEN, HIGH, LOW, CLOSE, VOLUME,

                    -- Lags
                    LAG(CLOSE, 1) OVER (ORDER BY DT) AS LAG_CLOSE_1,
                    LAG(CLOSE, 5) OVER (ORDER BY DT) AS LAG_CLOSE_5,

                    -- Returns
                    CASE WHEN LAG(CLOSE,1) OVER (ORDER BY DT) IS NULL THEN NULL
                         ELSE (CLOSE / LAG(CLOSE,1) OVER (ORDER BY DT)) - 1 END AS RET_1D,
                    CASE WHEN LAG(CLOSE,5) OVER (ORDER BY DT) IS NULL THEN NULL
                         ELSE (CLOSE / LAG(CLOSE,5) OVER (ORDER BY DT)) - 1 END AS RET_5D,

                    -- Rolling means
                    AVG(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  AS MA_3,
                    AVG(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS MA_7,
                    AVG(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS MA_14,
                    AVG(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30,

                    -- Rolling stddevs
                    STDDEV_SAMP(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS STD_7,
                    STDDEV_SAMP(CLOSE) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS STD_30,

                    -- Volume features
                    AVG(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS VOL_MA_7,
                    AVG(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS VOL_M_30,
                    STDDEV_SAMP(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS VOL_S_30,
                    CASE
                      WHEN STDDEV_SAMP(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 0 THEN NULL
                      ELSE (VOLUME - AVG(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
                           / NULLIFZERO(STDDEV_SAMP(VOLUME) OVER (ORDER BY DT ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
                    END AS VOL_Z_30,

                    -- Momentum (RSI_14, simple-window style)
                    CASE
                      WHEN (AVG(IFF(delta<0, -delta, 0)) OVER (ORDER BY DT ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)) = 0
                      THEN NULL
                      ELSE 100 - (100 / (1 + (
                          AVG(IFF(delta>0,  delta, 0)) OVER (ORDER BY DT ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
                          / NULLIFZERO(AVG(IFF(delta<0, -delta, 0)) OVER (ORDER BY DT ROWS BETWEEN 13 PRECEDING AND CURRENT ROW))
                      )))
                    END AS RSI_14,

                    -- Sentiment (daily + short rollups)
                    SENT_MEAN,
                    SENT_COUNT,
                    AVG(SENT_MEAN) OVER (ORDER BY DT ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS SENT_MEAN_3D,
                    AVG(SENT_MEAN) OVER (ORDER BY DT ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS SENT_MEAN_7D,

                    CURRENT_TIMESTAMP() AS CREATED_AT

                  FROM (
                    SELECT
                      J.*,
                      (CLOSE - LAG(CLOSE,1) OVER (ORDER BY DT)) AS delta
                    FROM J
                  )
                )
                SELECT * FROM F
                ORDER BY DT;
                """

                # Execute staging build
                cur.execute(sql_staging)

                # MERGE into final per-ticker features table
                merge_sql = f"""
                MERGE INTO {feat_tbl} t
                USING {temp_tbl} s
                ON t.SYMBOL = s.SYMBOL AND t.DT = s.DT
                WHEN MATCHED THEN UPDATE SET
                  OPEN=s.OPEN, HIGH=s.HIGH, LOW=s.LOW, CLOSE=s.CLOSE, VOLUME=s.VOLUME,
                  LAG_CLOSE_1=s.LAG_CLOSE_1, LAG_CLOSE_5=s.LAG_CLOSE_5,
                  RET_1D=s.RET_1D, RET_5D=s.RET_5D,
                  MA_3=s.MA_3, MA_7=s.MA_7, MA_14=s.MA_14, MA_30=s.MA_30,
                  STD_7=s.STD_7, STD_30=s.STD_30,
                  VOL_MA_7=s.VOL_MA_7, VOL_M_30=s.VOL_M_30, VOL_S_30=s.VOL_S_30, VOL_Z_30=s.VOL_Z_30,
                  RSI_14=s.RSI_14,
                  SENT_MEAN=s.SENT_MEAN, SENT_COUNT=s.SENT_COUNT,
                  SENT_MEAN_3D=s.SENT_MEAN_3D, SENT_MEAN_7D=s.SENT_MEAN_7D,
                  CREATED_AT=CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                  SYMBOL, DT, OPEN, HIGH, LOW, CLOSE, VOLUME,
                  LAG_CLOSE_1, LAG_CLOSE_5, RET_1D, RET_5D,
                  MA_3, MA_7, MA_14, MA_30,
                  STD_7, STD_30,
                  VOL_MA_7, VOL_M_30, VOL_S_30, VOL_Z_30,
                  RSI_14,
                  SENT_MEAN, SENT_COUNT, SENT_MEAN_3D, SENT_MEAN_7D,
                  CREATED_AT
                ) VALUES (
                  s.SYMBOL, s.DT, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME,
                  s.LAG_CLOSE_1, s.LAG_CLOSE_5, s.RET_1D, s.RET_5D,
                  s.MA_3, s.MA_7, s.MA_14, s.MA_30,
                  s.STD_7, s.STD_30,
                  s.VOL_MA_7, s.VOL_M_30, s.VOL_S_30, s.VOL_Z_30,
                  s.RSI_14,
                  s.SENT_MEAN, s.SENT_COUNT, s.SENT_MEAN_3D, s.SENT_MEAN_7D,
                  s.CREATED_AT
                );
                """
                cur.execute(merge_sql)

                # Rough row count touched (optional)
                cur.execute(f"SELECT COUNT(*) FROM {temp_tbl}")
                cnt = cur.fetchone()[0]
                total_rows += int(cnt)

        conn.commit()
        logger.info("✅ Feature build complete. Rows staged across symbols: %d", total_rows)
        return total_rows

    except Exception as e:
        conn.rollback()
        logger.error("❌ DML phase failed; rolled back: %s", e, exc_info=True)
        raise
    finally:
        conn.close()

# ----------------------------- DAG Definition -----------------------------

with DAG(
    dag_id="common_features_per_ticker_etl",
    description="Build per-ticker model-ready feature tables (price + sentiment) in Snowflake",
    start_date=timezone.datetime(2025, 10, 2),
    schedule="10 6 * * *",  # after RAW load @ 06:00; before training/prediction
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ETL", "features", "snowflake", "sentiment"],
) as dag:

    cfg = resolve_config()
    build_features(cfg)
