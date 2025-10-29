from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def execute_ctas(schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!! Primary key uniqueness failed !!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            

        duplicate_check_sql = f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT USERID, SESSIONID, CHANNEL, TS) AS distinct_rows
            FROM {schema}.temp_{table}
        """
        logging.info(f"Duplicate check SQL:\n{duplicate_check_sql}")
        cur.execute(duplicate_check_sql)
        total_rows, unique_rows = cur.fetchone()
        if total_rows > unique_rows:
            raise Exception(f"Duplicate rows detected: {total_rows - unique_rows} duplicates")

        # Creating main table if not exists 
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        # Swapping the tables
        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id='elt_airflow_homework_7',
    start_date=datetime(2024, 10, 24),
    catchup=False,
    tags=['ELT using Airflow'],
    schedule_interval='@once',
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT usr.*, stimestmp.ts
    FROM raw.user_session_channel usr
    JOIN raw.session_timestamp stimestmp ON usr.sessionId=stimestmp.sessionId
    """

    execute_ctas(schema, table, select_sql, primary_key='sessionId')