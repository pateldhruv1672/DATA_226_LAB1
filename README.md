# Stock Price Forecasting using Snowflake and Airflow

This repository contains an automated, end‑to‑end pipeline that ingests daily stock data, loads it into Snowflake, and trains per‑symbol forecasting models using Snowflake’s native ML capabilities. Orchestration is handled by Apache Airflow.

## Highlights
- Daily ETL of OHLCV data (per ticker) into Snowflake
- Two‑DAG architecture: one for ETL, one for model training + forecasting (parallel per symbol)
- Dynamic task mapping + Airflow Pools for controlled concurrency
- Transactional SQL operations (idempotent loads; safe retries)
- Final tables that merge historical + forecasted values for downstream analytics

## Architecture Overview
**DAG 1 — `yfinance_etl`**
- **resolve_config**: Reads config (tickers, lookback, target schema) from Airflow Variables and shares it downstream.
- **extract**: Pulls daily OHLCV for each ticker from yfinance; normalizes columns.
- **transform**: Casts types, converts dates to ISO strings, aligns field names to target schema.
- **load**: Ensures schema/tables exist; performs transactional load (stage → MERGE → commit / rollback on error).
- **trigger_train_forecast**: Triggers the second DAG only after a successful ETL run.

**DAG 2 — `snowflake_train_and_forecast_parallel`**
- **resolve_config**: Reconstructs training/forecast parameters (schemas, windows, horizon, symbols).
- **fan‑out training**: Expands to per‑symbol model‑training tasks.
- **fan‑out forecasting**: Expands to per‑symbol inference tasks (each depends on its matching training task).
- **train_one**: Builds/refreshes a view of recent closes and trains a Snowflake time‑series model (transactional).
- **forecast_one**: Deletes overlapping forecasts, generates new predictions + confidence bounds, inserts into forecast table, and creates/updates a final table that unions recent history with forecasts (transactional).

## Parallelization & Concurrency
- Uses Airflow **dynamic task mapping** to run per‑symbol tasks concurrently.
- Concurrency is throttled via an **Airflow Pool** (e.g., `snowflake_pool`) to avoid overloading Snowflake.
- Operations are **idempotent** and **transactional**, enabling safe retries.

## Airflow Configuration
- **Variables**: Snowflake connection/account details and operational knobs like `yf_lookback_days`, `yf_target_schema`, `yf_tickers`.
- **Pools**: Define `snowflake_pool` (e.g., 8 slots) in addition to the default pool.
- **Connections**: Configure `snowflake_conn` with account, warehouse, database, role, and credentials in the “Extra” JSON.

## Snowflake Schema Design
Two logical schemas:
- **RAW**: Per‑symbol historical OHLCV tables, e.g., `RAW.AAPL`, `RAW.GOOG`.
- **ANALYTICS**:
  - Forecast tables: `<SYMBOL>_FORECAST` with predicted close and confidence bounds.
  - Training views: `V_TRAIN_<SYMBOL>` referencing recent RAW data.
  - Final tables: `<SYMBOL>_FINAL` merging recent historical + forecast rows.

## Outputs
After a successful run you’ll have:
- `RAW.<SYMBOL>` tables with daily OHLCV
- `ANALYTICS.<SYMBOL>_FORECAST` tables with predictions
- `ANALYTICS.V_TRAIN_<SYMBOL>` training views
- `<SYMBOL>_FINAL` tables that combine historical + forecasts
- Airflow UI showing successful DAG runs and parallel task mapping

## Quickstart
1. **Set Airflow Variables** for tickers, lookback days, and target schemas.
2. **Create Airflow Pool** (e.g., `snowflake_pool`) to cap Snowflake sessions.
3. **Configure Connection** `snowflake_conn` with account, warehouse, database, role, user, and password (Extras JSON as needed).
4. **Deploy DAGs**: place them under `dags/` and restart or refresh your Airflow installation.
5. **Trigger**: run `yfinance_etl` from the Airflow UI; it will trigger the training/forecast DAG on success.

## Example Repository Layout
```
.
├─ dags/
│  ├─ yfinance_etl.py
│  └─ snowflake_train_and_forecast_parallel.py
├─ sql/                # optional helpers for views/tables
├─ screenshots/        # UI screenshots for your report
└─ README.md
```

## Authors
- Drashti Shah
- Dhruvkumar Patel

## References
- yfinance — https://pypi.org/project/yfinance/
- Apache Airflow — https://airflow.apache.org/
- Snowflake Machine Learning — https://docs.snowflake.com/en/user-guide/ml
- pandas — https://pandas.pydata.org/
