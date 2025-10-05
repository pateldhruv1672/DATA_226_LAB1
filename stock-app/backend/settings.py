# backend/settings.py
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent
BACKEND = Path(__file__).resolve().parent

class Settings(BaseSettings):
    # --- Snowflake ---
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: str
    SNOWFLAKE_WAREHOUSE: str
    SNOWFLAKE_DATABASE: str = "USER_DB_GOPHER"
    SNOWFLAKE_SCHEMA: str = "ANALYTICS"
    SNOWFLAKE_ROLE: str | None = None

    # --- Columns ---
    COL_TICKER: str = "SYMBOL"
    COL_DATE: str = "DT"
    COL_CLOSE: str = "CLOSE"
    COL_CLOSE_PRED: str = "CLOSE_FORECAST"

    # --- Airflow ---
    AIRFLOW_BASE_URL: str | None = None
    AIRFLOW_DAG_ID: str | None = None
    AIRFLOW_USERNAME: str | None = None
    AIRFLOW_PASSWORD: str | None = None
    AIRFLOW_TOKEN: str | None = None
    AIRFLOW_TIMEOUT: int = 20
    AIRFLOW_VERIFY_TLS: bool = True
    AIRFLOW_VAR_TICKER_KEY: str = "yf_ticker"

    # --- Polling ---
    POLL_INTERVAL_SECONDS: int = 3
    POLL_TIMEOUT_SECONDS: int = 300
    TABLE_SUFFIX: str = "_FINAL"

    # pydantic-settings v2 config
    model_config = SettingsConfigDict(
        env_file=[str(ROOT / ".env"), str(BACKEND / ".env")],
        env_file_encoding="utf-8",
        extra="ignore",                 # 👈 ignore unknown keys like vantage_api
    )

    def table_for_ticker(self, ticker: str) -> str:
        return f"{ticker.strip().upper()}{self.TABLE_SUFFIX}"
