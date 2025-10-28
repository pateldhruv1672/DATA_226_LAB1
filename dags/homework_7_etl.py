"""
Airflow DAG: Transactional insert/copy with a separate create_table task.

- `create_table` ensures the table exists (no transaction needed).
- `insert_data` runs in an explicit transaction (BEGIN/COMMIT/ROLLBACK) and supports:
    * COPY FROM external URL (e.g., S3) via a TEMP stage; or
    * direct INSERT SQL when `url` is None.

Configure the constants at the top or pass parameters on task invocation.
"""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Iterable, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ========= Constants =========
CONN_ID = "snowflake_conn"  # Airflow Snowflake connection id to use
DEFAULT_SCHEMA = "RAW"
DEFAULT_DB: Optional[str] = "USER_DB_GOPHER"  # Set if you want to force a DB context: e.g. "YOUR_DB"

# ========= Connection Helper =========

def get_sf_conn(conn_id: str = CONN_ID):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    return hook.get_conn()  # snowflake.connector.connection.SnowflakeConnection

# ========= Tasks =========

@task
def create_table(schema: str, table: str, table_definition_sql: str, conn_id: str = CONN_ID) -> str:
    """Create (or replace) a table. Separate from data load for clearer idempotency."""
    conn = get_sf_conn(conn_id)
    cur = conn.cursor()
    fq_table = f"{schema}.{table}"
    try:
        if DEFAULT_DB:
            cur.execute(f"USE DATABASE {DEFAULT_DB}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute(f"CREATE OR REPLACE TABLE {fq_table} ({table_definition_sql})")
        logging.info("Table %s created or replaced successfully.", fq_table)
        return f"Table {fq_table} created."
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@task
def insert_data(
    schema: str,
    table: str,
    stage_name: str = "RAW.BLOB_STAGE",
    files: Optional[Iterable[str]] = None,
    insert_sql: Optional[str] = None,
    conn_id: str = CONN_ID,
) -> str:
    """
    Transactional load using an *existing* named stage (e.g., RAW.BLOB_STAGE) — no temp stages.

    Provide either:
      - `files`: iterable of file names (e.g., ["user_session_channel.csv"]) that exist under the stage URL
      - `insert_sql`: a direct INSERT statement when not copying from stage

    The stage should already carry the correct FILE_FORMAT (as in your `CREATE OR REPLACE STAGE RAW.BLOB_STAGE ...`).
    """
    if not files and not insert_sql:
        raise ValueError("Provide either `files` for COPY or `insert_sql` for INSERT.")

    conn = get_sf_conn(conn_id)
    cur = conn.cursor()
    fq_table = f"{schema}.{table}"
    cur.excecute(f"CREATE OR REPLACE STAGE {stage_name}")  # Ensure stage exists
    try:
        logging.info("Starting transaction for %s...", fq_table)
        cur.execute("BEGIN")

        if DEFAULT_DB:
            cur.execute(f"USE DATABASE {DEFAULT_DB}")
        cur.execute(f"USE SCHEMA {schema}")

        if files:
            for fname in files:
                copy_sql = (
                    f"COPY INTO {fq_table} "
                    f"FROM @{stage_name}/{fname} "
                    "ON_ERROR = 'ABORT_STATEMENT'"
                )
                logging.info("COPY SQL:%s", copy_sql)
                cur.execute(copy_sql)

        cur.execute("COMMIT")
        logging.info("Transaction committed for %s.", fq_table)
        return f"Loaded data into {fq_table}."

    except Exception:
        logging.exception("Error during transaction for %s. Rolling back.", fq_table)
        try:
            cur.execute("ROLLBACK")
        except Exception as rb_err:
            logging.exception("Rollback failed: %s", rb_err)
        raise


# ========= Example DAG wiring =========
with DAG(
    dag_id="snowflake_create_and_load_separate_tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "copy", "transaction", "raw"],
) as dag:

    ct_user_session_channel = create_table(
        schema=DEFAULT_SCHEMA,
        table="USER_SESSION_CHANNEL",
        table_definition_sql="""
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'  
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
            sessionId varchar(32) primary key,
            ts timestamp  
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