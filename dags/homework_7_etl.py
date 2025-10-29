from __future__ import annotations

from datetime import datetime, timedelta
import logging
from typing import Iterable, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ========= Constants =========
CONN_ID = "snowflake_conn"
DEFAULT_SCHEMA = "RAW"
DEFAULT_DB: Optional[str] = "USER_DB_GOPHER"  # or None

def get_sf_conn(conn_id: str = CONN_ID):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    return hook.get_conn()

@task
def create_table(schema: str, table: str, table_definition_sql: str, conn_id: str = CONN_ID) -> str:
    conn = get_sf_conn(conn_id)
    cur = conn.cursor()
    fq_table = f"{schema}.{table}"
    try:
        if DEFAULT_DB:
            cur.execute(f"USE DATABASE {DEFAULT_DB}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute(f"CREATE OR REPLACE TABLE {fq_table} ({table_definition_sql})")
        logging.info("Table %s created or replaced.", fq_table)
        return f"Table {fq_table} created."
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

@task
def insert_data(
    schema: str,
    table: str,
    stage_name: str = "RAW.BLOB_STAGE",
    files: Optional[Iterable[str]] = None,
    insert_sql: Optional[str] = None,
    conn_id: str = CONN_ID,
) -> str:
    if not files and not insert_sql:
        raise ValueError("Provide either `files` for COPY or `insert_sql` for INSERT.")

    conn = get_sf_conn(conn_id)
    cur = conn.cursor()
    fq_table = f"{schema}.{table}"

    try:
        if DEFAULT_DB:
            cur.execute(f"USE DATABASE {DEFAULT_DB}")
        cur.execute(f"USE SCHEMA {schema}")

        # Ensure an internal stage exists (remove/replace with URL ... FILE_FORMAT (...) if using external S3)
        cur.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

        logging.info("Starting transaction for %s...", fq_table)
        cur.execute("BEGIN")

        if files:
            for fname in files:
                copy_sql = (
                    f"COPY INTO {fq_table} "
                    f"FROM @{stage_name}/{fname} "
                    "ON_ERROR = 'ABORT_STATEMENT'"
                )
                logging.info("COPY SQL: %s", copy_sql)
                cur.execute(copy_sql)
        else:
            cur.execute(insert_sql)  # trust caller to provide safe SQL

        cur.execute("COMMIT")
        logging.info("Transaction committed for %s.", fq_table)
        return f"Loaded data into {fq_table}."
    except Exception:
        logging.exception("Error during transaction for %s. Rolling back.", fq_table)
        try: cur.execute("ROLLBACK")
        except Exception as rb_err:
            logging.exception("Rollback failed: %s", rb_err)
        raise
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

with DAG(
    dag_id="etl_session_data_to_snowflake",
    start_date=datetime(2025, 10, 27),
    catchup=False,
    tags=["ETL"],
    schedule="@once",  # new-style parameter
    default_args={},
) as dag:

    ct_user_session_channel = create_table(
        schema=DEFAULT_SCHEMA,
        table="USER_SESSION_CHANNEL",
        table_definition_sql="""
            userId INT NOT NULL,
            sessionId VARCHAR(32) PRIMARY KEY,
            channel VARCHAR(32) DEFAULT 'direct'
        """,
    )

    load_user_session_channel = insert_data(
        schema=DEFAULT_SCHEMA,
        table="USER_SESSION_CHANNEL",
        stage_name="RAW.BLOB_STAGE",
        files=["user_session_channel.csv"],
    )

    ct_session_timestamp = create_table(
        schema=DEFAULT_SCHEMA,
        table="SESSION_TIMESTAMP",
        table_definition_sql="""
            sessionId VARCHAR(32) PRIMARY KEY,
            ts TIMESTAMP
        """,
    )

    load_session_timestamp = insert_data(
        schema=DEFAULT_SCHEMA,
        table="SESSION_TIMESTAMP",
        stage_name="RAW.BLOB_STAGE",
        files=["session_timestamp.csv"],
    )

    ct_user_session_channel >> load_user_session_channel
    ct_session_timestamp >> load_session_timestamp
