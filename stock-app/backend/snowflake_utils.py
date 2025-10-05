from __future__ import annotations
import functools, logging, time
from typing import Iterable, List, Optional, Tuple, TypeVar
import snowflake.connector
from snowflake.connector import errors as sf_errors
from backend.settings import Settings

T = TypeVar("T")
logger = logging.getLogger(__name__)

def _retry(*, tries=3, base_delay=0.5, max_delay=4.0,
           retry_on: Iterable[type] = (sf_errors.OperationalError, sf_errors.DatabaseError, sf_errors.InterfaceError)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            delay, remaining = base_delay, tries
            while True:
                try: return fn(*args, **kwargs)
                except retry_on as exc:
                    remaining -= 1
                    if remaining <= 0:
                        logger.exception("Snowflake call failed after retries")
                        raise
                    time.sleep(delay); delay = min(max_delay, delay * 2)
        return wrapper
    return deco

def _quote_ident(ident: str) -> str:
    s = str(ident)
    if s and all(c.isalnum() or c in ("_", "$") for c in s):
        return s.upper()
    return f"\"{s.replace('\"','\"\"')}\""

def _fqtn(db: str, sc: str, tb: str) -> str:
    return f"{_quote_ident(db)}.{_quote_ident(sc)}.{_quote_ident(tb)}"

class SnowflakeClient:
    def __init__(self, settings: Settings):
        self.s = settings
        self._db, self._schema = self.s.SNOWFLAKE_DATABASE, self.s.SNOWFLAKE_SCHEMA
        self._warehouse = self.s.SNOWFLAKE_WAREHOUSE
        self._role = self.s.SNOWFLAKE_ROLE or None

    def _conn(self):
        return snowflake.connector.connect(
            account=self.s.SNOWFLAKE_ACCOUNT,
            user=self.s.SNOWFLAKE_USER,
            password=self.s.SNOWFLAKE_PASSWORD,
            warehouse=self._warehouse,
            database=self._db,
            schema=self._schema,
            role=self._role,
            client_session_keep_alive=False,
            autocommit=True,
            session_parameters={
                "QUERY_TAG": "stocks-api",
                "STATEMENT_TIMEOUT_IN_SECONDS": 60,
            },
        )

    @_retry()
    def table_exists(self, table_name: str) -> bool:
        sql = ("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
               "WHERE TABLE_CATALOG=%s AND TABLE_SCHEMA=%s AND TABLE_NAME=%s")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (self._db.upper(), self._schema.upper(), table_name.upper()))
            (count,) = cur.fetchone()
            return count > 0

    @_retry()
    def fetch_company_series(self, table_name: str) -> List[Tuple[str, float, float]]:
        fqtn = _fqtn(self._db, self._schema, table_name)
        d = _quote_ident(self.s.COL_DATE)
        c = _quote_ident(self.s.COL_CLOSE)
        p = _quote_ident(self.s.COL_CLOSE_PRED)
        sql = (f"SELECT TO_VARCHAR({d}) AS dt, {c} AS close, {p} AS close_predicted "
               f"FROM {fqtn} ORDER BY {d}")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall() or []
            return [(str(dt), float(close) if close is not None else None,
                     float(pred) if pred is not None else None) for dt, close, pred in rows]
