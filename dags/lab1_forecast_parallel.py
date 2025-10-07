# dag_snowflake_train_and_forecast_parallel.py
# Airflow 2.x, Python 3.10+
# Parallelized per-symbol TRAIN -> FORECAST using dynamic task mapping + Pools
# - TRAIN: build ANALYTICS.V_TRAIN_<SAFE>, (re)create ANALYTICS.MODEL_FORECAST_<SAFE>
# - FORECAST: write ANALYTICS."<SYMBOL>_FORECAST", rebuild ANALYTICS."<SYMBOL>_FINAL"
# Assumes RAW."<SYMBOL>" tables already exist.

from __future__ import annotations
from datetime import timedelta
import json
import logging
import re
from typing import Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# ----------------------------- Constants & Defaults -----------------------------

DEFAULT_ARGS = {
    "owner": "Dhruv-Drashti",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_POOL = "snowflake_pool"  # Create this pool in Airflow UI to cap concurrency

# Airflow Variables
VAR_HORIZON_DAYS = "forecast_horizon_days"               # e.g. "14"
VAR_LOOKBACK_DAYS = "train_lookback_days"                # e.g. "180"
VAR_SYMBOLS = "yf_tickers"                               # e.g. '["NVDA","AAPL"]'
VAR_SOURCE_SCHEMA = "sf_source_schema"                   # default RAW
VAR_ANALYTICS_SCHEMA = "sf_analytics_schema"             # default ANALYTICS
VAR_PREDICTION_INTERVAL = "forecast_prediction_interval" # e.g. "0.95"

# Defaults
DEFAULT_HORIZON_DAYS = 14
DEFAULT_LOOKBACK_DAYS = 180
DEFAULT_SYMBOLS = ["NVDA", "AAPL"]
DEFAULT_SOURCE_SCHEMA = "RAW"
DEFAULT_ANALYTICS_SCHEMA = "ANALYTICS"
DEFAULT_PREDICTION_INTERVAL = 0.95

# ----------------------------- Helper Functions -----------------------------

def return_snowflake_conn():
    """Return a Snowflake DB-API connection via Airflow's SnowflakeHook."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

def _get_list_variable(key: str, default_list: List[str]) -> List[str]:
    try:
        raw = Variable.get(key, default_var=json.dumps(default_list))
        val = json.loads(raw) if isinstance(raw, str) else raw
        return [str(s).strip().upper() for s in val] if isinstance(val, list) else default_list
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

def _get_float_variable(key: str, default_float: float) -> float:
    try:
        return float(Variable.get(key, default_var=str(default_float)))
    except Exception:
        return default_float

def qident(ident: str) -> str:
    """Safely quote a Snowflake identifier (handles dots like BRK.B)."""
    return f"\"{ident.replace('\"', '\"\"')}\""

def safe_name(name: str) -> str:
    """Safe object suffix (models/views) by replacing non-alphanumerics."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

def ensure_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

def ensure_per_symbol_tables(conn, analytics_schema: str, symbol: str) -> None:
    preds_tbl = f"{analytics_schema}.{qident(symbol + '_FORECAST')}"
    final_tbl = f"{analytics_schema}.{qident(symbol + '_FINAL')}"
    with conn.cursor() as cur:
        # Forecast (predictions) table
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {preds_tbl} (
                SYMBOL STRING,
                TS TIMESTAMP_NTZ,
                CLOSE_PRED FLOAT,
                LOWER_BOUND FLOAT,
                UPPER_BOUND FLOAT,
                PREDICTION_FOR DATE,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        # Final UNION table (RAW + FORECAST); standardized bound names
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {final_tbl} (
                SYMBOL STRING,
                DT DATE,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                VOLUME NUMBER,
                SOURCE STRING,
                CLOSE_FORECAST FLOAT,
                UPPER_BOUND_FORECAST FLOAT,
                LOWER_BOUND_FORECAST FLOAT,
                CREATED_AT TIMESTAMP_NTZ
            )
            """
        )

# ----------------------------- Tasks -----------------------------

@task
def resolve_config() -> Dict:
    """Resolve runtime config from Airflow Variables and normalize."""
    logger = logging.getLogger("airflow.task")
    cfg = {
        "source_schema": _get_str_variable(VAR_SOURCE_SCHEMA, DEFAULT_SOURCE_SCHEMA),
        "analytics_schema": _get_str_variable(VAR_ANALYTICS_SCHEMA, DEFAULT_ANALYTICS_SCHEMA),
        "horizon_days": _get_int_variable(VAR_HORIZON_DAYS, DEFAULT_HORIZON_DAYS),
        "lookback_days": _get_int_variable(VAR_LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS),
        "symbols": _get_list_variable(VAR_SYMBOLS, DEFAULT_SYMBOLS),
        "prediction_interval": _get_float_variable(VAR_PREDICTION_INTERVAL, DEFAULT_PREDICTION_INTERVAL),
    }
    logger.info("Resolved cfg: %s", cfg)
    return cfg

@task
def fanout_train(cfg: Dict) -> List[Dict]:
    """Build kwargs list (one per symbol) for training."""
    return [
        {
            "source_schema": cfg["source_schema"],
            "analytics_schema": cfg["analytics_schema"],
            "lookback_days": cfg["lookback_days"],
            "symbol": sym,
        }
        for sym in cfg["symbols"]
    ]

@task
def fanout_forecast(cfg: Dict) -> List[Dict]:
    """Build kwargs list (one per symbol) for forecasting."""
    return [
        {
            "source_schema": cfg["source_schema"],
            "analytics_schema": cfg["analytics_schema"],
            "horizon_days": cfg["horizon_days"],
            "lookback_days": cfg["lookback_days"],
            "prediction_interval": cfg["prediction_interval"],
            "symbol": sym,
        }
        for sym in cfg["symbols"]
    ]

@task(task_id="stage_train_one", pool=SNOWFLAKE_POOL, pool_slots=1, retries=1)
def stage_train_one(kwargs: Dict) -> str:
    """
    Train model for ONE symbol:
      - CREATE OR REPLACE VIEW ANALYTICS.V_TRAIN_<SYMBOL>
      - CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MODEL_FORECAST_<SYMBOL>
    """
    logger = logging.getLogger("airflow.task")
    src_schema = kwargs["source_schema"]
    analytics_schema = kwargs["analytics_schema"]
    lookback = kwargs["lookback_days"]
    sym = kwargs["symbol"]

    conn = return_snowflake_conn()
    try:
        ensure_schema(conn, analytics_schema)
        with conn.cursor() as cur:
            cur.execute(f"USE SCHEMA {analytics_schema}")

            safe_sym = safe_name(sym)
            raw_tbl = f"{src_schema}.{qident(sym)}"
            v_train_fqn = f"{analytics_schema}.V_TRAIN_{safe_sym}"
            model_name = f"MODEL_FORECAST_{safe_sym}"

            try:
                cur.execute("BEGIN")

                # Training view with TIMESTAMP_NTZ
                cur.execute(
                    f"""
                    CREATE OR REPLACE VIEW {v_train_fqn} AS
                    SELECT
                        DT::TIMESTAMP_NTZ AS TS,
                        CLOSE
                    FROM {raw_tbl}
                    WHERE DT >= DATEADD('day', -{lookback}, CURRENT_DATE())
                      AND CLOSE IS NOT NULL
                    """
                )

                # Sanity: count training rows
                cur.execute(f"SELECT COUNT(*) FROM {v_train_fqn}")
                train_rows = cur.fetchone()[0]
                logger.info("[TRAIN] %s: rows=%s in %s", sym, train_rows, v_train_fqn)

                # Create/replace model
                cur.execute(
                    f"""
                    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name}(
                      INPUT_DATA        => TABLE({v_train_fqn}),
                      TIMESTAMP_COLNAME => 'TS',
                      TARGET_COLNAME    => 'CLOSE',
                      CONFIG_OBJECT     => {{ 'ON_ERROR': 'SKIP' }}
                    )
                    """
                )

                cur.execute("COMMIT")
                logger.info("✅ [TRAIN] %s: model %s refreshed", sym, model_name)
                return f"{sym}: ok(train_rows={train_rows})"

            except Exception as e:
                logger.error("❌ [TRAIN] %s failed: %s", sym, e, exc_info=True)
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                raise
    finally:
        conn.close()

@task(task_id="stage_forecast_one", pool=SNOWFLAKE_POOL, pool_slots=1, retries=1)
def stage_forecast_one(kwargs: Dict) -> str:
    """
    Forecast for ONE symbol:
      - Ensure tables
      - DELETE today+ predictions for idempotency
      - INSERT from model!FORECAST()
      - CREATE OR REPLACE <SYMBOL>_FINAL as RAW (lookback) UNION ALL FORECAST (today+)
    """
    logger = logging.getLogger("airflow.task")
    src_schema = kwargs["source_schema"]
    analytics_schema = kwargs["analytics_schema"]
    horizon = kwargs["horizon_days"]
    lookback = kwargs["lookback_days"]
    p_interval = kwargs["prediction_interval"]
    sym = kwargs["symbol"]

    conn = return_snowflake_conn()
    try:
        ensure_schema(conn, analytics_schema)
        ensure_per_symbol_tables(conn, analytics_schema, sym)

        with conn.cursor() as cur:
            cur.execute(f"USE SCHEMA {analytics_schema}")

            safe_sym = safe_name(sym)
            raw_tbl   = f"{src_schema}.{qident(sym)}"
            model_name = f"MODEL_FORECAST_{safe_sym}"
            preds_tbl = f"{analytics_schema}.{qident(sym + '_FORECAST')}"
            final_tbl = f"{analytics_schema}.{qident(sym + '_FINAL')}"

            try:
                cur.execute("BEGIN")

                # Clean predictions for today+ (idempotent)
                cur.execute(f"DELETE FROM {preds_tbl} WHERE PREDICTION_FOR >= CURRENT_DATE()")

                # Insert fresh predictions
                cur.execute(
                    f"""
                    INSERT INTO {preds_tbl} (SYMBOL, TS, CLOSE_PRED, LOWER_BOUND, UPPER_BOUND, PREDICTION_FOR)
                    SELECT
                        '{sym}' AS SYMBOL,
                        TS,
                        FORECAST         AS CLOSE_PRED,
                        LOWER_BOUND,
                        UPPER_BOUND,
                        CAST(TS AS DATE) AS PREDICTION_FOR
                    FROM TABLE(
                        {model_name}!FORECAST(
                            FORECASTING_PERIODS => {horizon},
                            CONFIG_OBJECT       => {{ 'prediction_interval': {p_interval} }}
                        )
                    )
                    """
                )

                # Sanity: count predictions
                cur.execute(f"SELECT COUNT(*) FROM {preds_tbl} WHERE PREDICTION_FOR >= CURRENT_DATE()")
                pred_rows = cur.fetchone()[0]
                logger.info("[FORECAST] %s: predictions today+ = %s", sym, pred_rows)

                # Rebuild FINAL (RAW window + FORECAST)
                cur.execute(
                    f"""
                    CREATE OR REPLACE TABLE {final_tbl} AS
                    -- RAW portion
                    SELECT
                        SYMBOL,
                        DT,
                        OPEN,
                        HIGH,
                        LOW,
                        CLOSE,
                        VOLUME,
                        'RAW' AS SOURCE,
                        NULL::FLOAT AS CLOSE_FORECAST,
                        NULL::FLOAT AS UPPER_BOUND_FORECAST,
                        NULL::FLOAT AS LOWER_BOUND_FORECAST,
                        CURRENT_TIMESTAMP() AS CREATED_AT
                    FROM {raw_tbl}
                    WHERE DT >= DATEADD('day', -{lookback}, CURRENT_DATE())

                    UNION ALL

                    -- FORECAST portion
                    SELECT
                        '{sym}' AS SYMBOL,
                        PREDICTION_FOR AS DT,
                        NULL::FLOAT AS OPEN,
                        NULL::FLOAT AS HIGH,
                        NULL::FLOAT AS LOW,
                        NULL::FLOAT AS CLOSE,
                        NULL::NUMBER AS VOLUME,
                        'FORECAST' AS SOURCE,
                        CLOSE_PRED AS CLOSE_FORECAST,
                        UPPER_BOUND AS UPPER_BOUND_FORECAST,
                        LOWER_BOUND AS LOWER_BOUND_FORECAST,
                        CURRENT_TIMESTAMP() AS CREATED_AT
                    FROM {preds_tbl}
                    WHERE PREDICTION_FOR >= CURRENT_DATE()
                    """
                )

                cur.execute("COMMIT")
                logger.info("✅ [FORECAST] %s: FINAL rebuilt", sym)
                return f"{sym}: ok(pred_rows={pred_rows})"

            except Exception as e:
                logger.error("❌ [FORECAST] %s failed: %s", sym, e, exc_info=True)
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                raise
    finally:
        conn.close()

# ----------------------------- DAG Definition -----------------------------

with DAG(
    dag_id="snowflake_train_and_forecast_parallel",
    description="TRAIN models then FORECAST per symbol in ANALYTICS (parallel).",
    start_date=timezone.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "forecast", "parallel"],
) as dag:

    cfg = resolve_config()

    train_kwargs = fanout_train(cfg)
    forecast_kwargs = fanout_forecast(cfg)

    # Map per symbol
    trained = stage_train_one.expand(kwargs=train_kwargs)
    forecasted = stage_forecast_one.expand(kwargs=forecast_kwargs)

    # Align indices: forecast[i] waits for train[i]
    trained >> forecasted
