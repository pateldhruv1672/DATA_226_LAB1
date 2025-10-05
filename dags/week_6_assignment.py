from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from datetime import timedelta
from datetime import datetime, date
import snowflake.connector
import requests
import logging
from typing import List, Dict

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}
TARGET_SCHEMA = 'raw'
TARGET_TABLE = 'DAILY_PRICES_AIRFLOW'
SYMBOL= 'GOOG'

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn


@task
def extract(symbol, since_date):
    logger = logging.getLogger("airflow.task")
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={Variable.get("VANTAGE_API_KEY")}"
        r = requests.get(url)

        # check response code
        if r.status_code != 200:
            raise Exception(f"Error fetching data: {r.status_code} - {r.text}")

        data = r.json()
        ts = data.get("Time Series (Daily)")
        raw = []
        for d_str, vals in ts.items():
            d = date.fromisoformat(d_str)
            if d >= since_date:
                raw.append({
                    "symbol": symbol,
                    "date": d_str,                # keep explicit date (string) from API
                    "open": vals["1. open"],
                    "high": vals["2. high"],
                    "low":  vals["3. low"],
                    "close": vals["4. close"],
                    "volume": vals["5. volume"],
                })

        # Sort ascending by date (nice to have)
        raw.sort(key=lambda r: r["date"])
        print("=====The Extracted Data =====")
        print("Total records Extracted from AlphaVantage is " +str(len(raw)))
        logger.info("===== Extraction Complete =====")
        logger.info("✅ Total records extracted for %s: %d", symbol, len(raw))

        # Log each row for debugging
        for r in raw:
            logger.info("Record: %s", r)
        return raw

    except Exception as e:
        print(f"Some error getting the data: {e}")
        logger.error("❌ Some error getting the data: %s", e, exc_info=True)
        return []


@task
def transform(raw_records: List[Dict]) -> List[Dict]:
    """
    Casts types, normalizes keys, and returns clean rows ready for DB insert.
    Ensures schema: symbol, date (YYYY-MM-DD), open, close, high, low, volume.
    """
    logger = logging.getLogger("airflow.task")

    clean = []
    for r in raw_records:
        record = {
            "symbol": r["symbol"],
            "date":   r["date"],
            "open":   float(r["open"]),
            "close":  float(r["close"]),
            "high":   float(r["high"]),
            "low":    float(r["low"]),
            "volume": int(r["volume"]),
        }
        clean.append(record)

    # Log summary
    logger.info("===== The Transformed Data ======")
    logger.info("✅ Total records transformed: %d", len(clean))

    # Log each record (helpful for debugging)
    for rec in clean:
        logger.info("Transformed record: %s", rec)

    return clean

def ensure_schema_and_table(conn) -> None:
    """
    Ensures RAW schema and RAW.DAILY_PRICES_AIRFLOW table exist with composite PK (symbol, date).
    """
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            SYMBOL STRING NOT NULL,
            DATE   DATE   NOT NULL,
            OPEN   NUMBER(18,4),
            CLOSE  NUMBER(18,4),
            HIGH   NUMBER(18,4),
            LOW    NUMBER(18,4),
            VOLUME NUMBER(38,0),
            CONSTRAINT PK_{TARGET_TABLE} PRIMARY KEY (SYMBOL, DATE)
        )
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
        cur.execute(ddl)

def table_row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
        return cur.fetchone()[0]
    

@task
def load(rows):
    """
    Deletes all rows from RAW.DAILY_PRICES_AIRFLOW and inserts the given rows inside a single
    transaction. Rolls back on any error. Returns number of inserted rows.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    ensure_schema_and_table(conn)
    if not rows:
        print("No rows to load.")
        return 0

    insert_sql = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
        (SYMBOL, DATE, OPEN, CLOSE, HIGH, LOW, VOLUME)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (r["symbol"], r["date"], r["open"], r["close"], r["high"], r["low"], r["volume"])
        for r in rows
    ]

    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
        # Execute the query and fetch results
        conn = hook.get_conn()
        ensure_schema_and_table(conn)
        with conn.cursor() as cur:
            
            # Step 5 (assignment): delete all before insert
            cur.execute(f"DELETE FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
            # Step 6: insert
            cur.executemany(insert_sql, params)
        conn.commit()
        print("=====The Data is loaded into Snowflake=====")
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(e)
        raise e

with DAG(
    dag_id="google_VANTAGE",
    start_date=timezone.datetime(2025, 10, 4),
    catchup=False,
    tags=["ETL"],
    schedule="@once",
) as dag:
    today = date.today()
    SINCE_DATE = today - timedelta(days=90)
    data = extract(SYMBOL,SINCE_DATE)
    lines = transform(data)
    output_rows = load(lines)