from __future__ import annotations
import asyncio, logging, time
from contextlib import asynccontextmanager
# backend/app.py
from fastapi.responses import StreamingResponse
import asyncio, json

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

from backend.schemas import GraphResponse, StatusResponse, TimeSeriesPoint
from backend.snowflake_utils import SnowflakeClient
from backend.airflow_client import AirflowClient
from backend.settings import Settings
from pathlib import Path



from fastapi import Body
import uvicorn


ENV_PATH = Path(__file__).with_name(".env")  # backend/.env
settings = Settings(_env_file=str(ENV_PATH))
logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.sf = SnowflakeClient(settings)
    app.state.af = AirflowClient(settings)
    yield

app = FastAPI(title="Stock Forecast API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-friendly; tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_sf() -> SnowflakeClient: return app.state.sf
def get_af() -> AirflowClient:   return app.state.af

class TriggerBody(BaseModel):
    ticker: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/status", response_model=StatusResponse)
async def status_endpoint(sf: SnowflakeClient = Depends(get_sf),
                          ticker: str = Query(..., min_length=1)):
    table = settings.table_for_ticker(ticker)
    exists = await run_in_threadpool(sf.table_exists, table)
    return StatusResponse(table_exists=exists)

@app.get("/graphs", response_model=GraphResponse)
async def graphs(ticker: str = Query(..., min_length=1),
                 sf: SnowflakeClient = Depends(get_sf),
                 af: AirflowClient = Depends(get_af)):
    """
    End-to-end flow:
    1) If {TICKER}_FINAL exists -> return full time series.
    2) Else -> set Airflow Variable 'yf_ticker' = ["TICKER"], trigger DAG,
       poll Snowflake until table appears, then return the data.
    """
    t = ticker.strip().upper()
    table = settings.table_for_ticker(t)
    print(table)

    # 1) If table exists: return data
    if await run_in_threadpool(sf.table_exists, table):
        rows = await run_in_threadpool(sf.fetch_company_series, table)
        points = [TimeSeriesPoint(date=r[0], close=r[1], close_predicted=r[2]) for r in rows]
        return GraphResponse(ticker=t, table=table, points=points)

    # 2) Table doesn't exist -> set Airflow var + trigger DAG
    ok = await run_in_threadpool(af.set_variable, settings.AIRFLOW_VAR_TICKER_KEY, [t])
    if not ok:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail=f"Failed to set Airflow variable '{settings.AIRFLOW_VAR_TICKER_KEY}'")

    dag_run_id = await run_in_threadpool(af.trigger_now, {"ticker": t})
    if not dag_run_id:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail="Failed to trigger Airflow DAG")

    # 3) Poll Snowflake until table shows up (bounded wait)
    deadline = time.time() + settings.POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        exists = await run_in_threadpool(sf.table_exists, table)
        if exists:
            rows = await run_in_threadpool(sf.fetch_company_series, table)
            points = [TimeSeriesPoint(date=r[0], close=r[1], close_predicted=r[2]) for r in rows]
            return GraphResponse(ticker=t, table=table, points=points)
        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)

    # Timed out waiting for pipeline
    raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                        detail=f"Timed out waiting for table {table} to be created by the pipeline")



@app.post("/trigger-and-wait", response_model=GraphResponse)
async def trigger_and_wait(body: TriggerBody = Body(...),
                           timeout_sec: int = 600,   # 10 minutes max
                           poll_sec: int = 3):
    # if table already exists, skip to fetch
    if sf.table_exists():
        rows = sf.fetch_timeseries(body.ticker or "")
        return GraphResponse(
            ticker=body.ticker or "",
            points=[TimeSeriesPoint(date=r[0], close=r[1], close_predicted=r[2]) for r in rows]
        )

    # trigger DAG
    dag_run_id = af.trigger_dag(run_conf={"ticker": body.ticker} if body.ticker else {})
    if not dag_run_id:
        raise HTTPException(status_code=502, detail="Failed to trigger Airflow DAG")

    # poll Airflow for completion
    deadline = time.monotonic() + timeout_sec
    last_state = "queued"
    while time.monotonic() < deadline:
        state = af.get_dag_run_state(dag_run_id) or "unknown"
        last_state = state
        if state == "success":
            # table may appear a moment after success; wait a bit for Snowflake to see it
            for _ in range(24):  # up to ~2 minutes
                if sf.table_exists():
                    rows = sf.fetch_timeseries(body.ticker or "")
                    return GraphResponse(
                        ticker=body.ticker or "",
                        points=[TimeSeriesPoint(date=r[0], close=r[1], close_predicted=r[2]) for r in rows]
                    )
                await asyncio.sleep(5)
            raise HTTPException(status_code=504, detail="Table not visible after DAG success")
        if state in {"failed", "error"}:
            raise HTTPException(status_code=502, detail=f"DAG run {dag_run_id} ended with state={state}")
        await asyncio.sleep(poll_sec)

    raise HTTPException(status_code=504, detail=f"Timed out waiting for DAG (last_state={last_state})")
# backend/app.py
from fastapi.responses import StreamingResponse
import asyncio, json, time

@app.get("/sse/trigger")
async def sse_trigger(ticker: str | None = None):
    """
    Starts the pipeline (if table missing) and streams progress via SSE.
    Streamed events:
      - event: started        data: {"dag_run_id": "..."}
      - (default) message     data: {"state": "running", "elapsed_sec": 12}
      - event: ready          data: {"message": "table exists"}
      - event: failed         data: {"state": "failed"}
      - event: done           data: {}
    """

    # If table already exists, stream 'ready' immediately.
    if sf.table_exists():
        async def already_ready():
            # Tell client to reconnect every 5s if needed (SSE hint)
            yield "retry: 5000\n\n"
            # Heartbeat to keep connection compatible
            yield ":\n\n"
            yield f"event: ready\ndata: {json.dumps({'message':'table exists'})}\n\n"
            yield "event: done\ndata: {}\n\n"
        return StreamingResponse(already_ready(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    # Otherwise, trigger Airflow DAG
    dag_run_id = af.trigger_dag(run_conf={"ticker": ticker} if ticker else {})
    if not dag_run_id:
        # Use regular HTTP error if we couldn't even start
        raise HTTPException(status_code=502, detail="Failed to trigger Airflow DAG")

    async def event_stream():
        # Client-side automatic retry suggestion
        yield "retry: 5000\n\n"

        # Let the client know we started
        yield f"event: started\ndata: {json.dumps({'dag_run_id': dag_run_id})}\n\n"

        # Poll airflow + heartbeat
        start = time.monotonic()
        while True:
            # Heartbeat comment line keeps some stacks from idling out
            yield ":\n\n"

            state = af.get_dag_run_state(dag_run_id) or "unknown"
            payload = {"state": state, "elapsed_sec": int(time.monotonic() - start)}
            # default event (onmessage)
            yield f"data: {json.dumps(payload)}\n\n"

            if state in {"success", "failed", "error"}:
                if state == "success":
                    # Snowflake visibility can lag a bit; wait until table is visible
                    # so the client can immediately fetch /graphs
                    for _ in range(24):  # up to ~2 minutes
                        if sf.table_exists():
                            yield f"event: ready\ndata: {json.dumps({'message':'table ready'})}\n\n"
                            yield "event: done\ndata: {}\n\n"
                            return
                        await asyncio.sleep(5)
                    # Success but table not visible—treat as transient failure
                    yield "event: failed\ndata: {\"reason\":\"table_not_visible\"}\n\n"
                    yield "event: done\ndata: {}\n\n"
                    return
                else:
                    yield f"event: failed\ndata: {json.dumps(payload)}\n\n"
                    yield "event: done\ndata: {}\n\n"
                    return

            await asyncio.sleep(5)

    headers = {
        "Cache-Control": "no-cache",
        # If you ever reverse-proxy, this helps with nginx; local dev doesn't need it.
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)



if __name__ == "__main__":
    uvicorn.run("backend.app:app", host="0.0.0.0", port=8000, reload=True)
