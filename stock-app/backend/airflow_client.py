from __future__ import annotations
import base64, json, logging, requests, datetime, uuid
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from backend.settings import Settings

logger = logging.getLogger(__name__)

class AirflowClient:
    def __init__(self, settings: Settings):
        self.s = settings
        self.base = self.s.AIRFLOW_BASE_URL.rstrip("/") if self.s.AIRFLOW_BASE_URL else None
        self.dag_id = self.s.AIRFLOW_DAG_ID
        self.timeout = self.s.AIRFLOW_TIMEOUT
        self.verify_tls = self.s.AIRFLOW_VERIFY_TLS
        self.extra_headers: Dict[str, str] = {}
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        ses = requests.Session()
        retry = Retry(total=3, read=3, connect=3, backoff_factor=0.5,
                      status_forcelist=(500,502,503,504), allowed_methods=("GET","POST","PATCH","HEAD"))
        adapter = HTTPAdapter(max_retries=retry)
        ses.mount("http://", adapter); ses.mount("https://", adapter)
        return ses

    def _auth_headers(self) -> Dict[str,str]:
        if self.s.AIRFLOW_TOKEN:
            return {"Authorization": f"Bearer {self.s.AIRFLOW_TOKEN}"}
        if self.s.AIRFLOW_USERNAME and self.s.AIRFLOW_PASSWORD:
            b = base64.b64encode(f"{self.s.AIRFLOW_USERNAME}:{self.s.AIRFLOW_PASSWORD}".encode()).decode()
            return {"Authorization": f"Basic {b}"}
        return {}

    def _headers(self) -> Dict[str,str]:
        return {"Accept":"application/json","Content-Type":"application/json", **self._auth_headers(), **self.extra_headers}

    def trigger_dag(self, run_conf: Optional[dict] = None) -> Optional[str]:
        if not self.base or not self.dag_id:
            logger.error("Airflow not configured")
            return None
        url = f"{self.base}/api/v1/dags/{self.dag_id}/dagRuns"
        try:
            r = self._session.post(url, headers=self._headers(), data=json.dumps({"conf": run_conf or {}}),
                                   timeout=self.timeout, verify=self.verify_tls)
        except requests.RequestException:
            logger.exception("Airflow trigger call failed")
            return None
        if r.status_code in (200, 201):
            try: data = r.json()
            except ValueError: return None
            return data.get("dag_run_id") or data.get("dagRunId")
        logger.error("Airflow trigger failed: %s %s", r.status_code, r.text[:300])
        return None
    
    def unpause_dag(self, dag_id: Optional[str] = None) -> bool:
        """
        Ensure DAG is unpaused so the scheduler will pick the manual run.
        Note: A manual run typically works even if paused, but unpausing eliminates edge cases.
        """
        dag = dag_id or self.dag_id
        if not self.base or not dag:
            return False
        url = f"{self.base}/api/v1/dags/{dag}"
        try:
            r = self._session.patch(
                url,
                headers=self._headers(),
                data=json.dumps({"is_paused": False}),
                timeout=self.timeout,
                verify=self.verify_tls,
            )
            if r.status_code in (200, 207):  # 207 may appear in some versions
                return True
            logger.error("Unpause DAG failed: %s %s", r.status_code, r.text[:500])
            return False
        except requests.RequestException:
            logger.exception("Error unpausing DAG")
            return False

    def trigger_now(
        self,
        conf: Optional[dict] = None,
        dag_id: Optional[str] = None,
        ensure_unpaused: bool = True,
        logical_date: Optional[str] = None,
        dag_run_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Trigger a DAG run immediately (manual run). Works for scheduled or unscheduled DAGs.
        - Optionally unpauses first.
        - Sets a current logical_date and a unique dag_run_id to avoid collisions.
        """
        dag = dag_id or self.dag_id
        if not self.base or not dag:
            logger.error("trigger_now: Airflow not configured")
            return None

        if ensure_unpaused and not self.unpause_dag(dag):
            logger.warning("trigger_now: failed to unpause DAG %s (continuing anyway)", dag)

        if logical_date is None:
            # Airflow expects RFC3339/ISO-8601 with Z
            logical_date = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        if dag_run_id is None:
            dag_run_id = f"manual__{uuid.uuid4()}"

        url = f"{self.base}/api/v1/dags/{dag}/dagRuns"
        payload = {"conf": conf or {}, "logical_date": logical_date, "dag_run_id": dag_run_id}

        try:
            r = self._session.post(
                url,
                headers=self._headers(),
                data=json.dumps(payload),
                timeout=self.timeout,
                verify=self.verify_tls,
            )
        except requests.RequestException:
            logger.exception("trigger_now: POST failed")
            return None

        if r.status_code in (200, 201):
            try:
                data = r.json()
            except ValueError:
                logger.error("trigger_now: non-JSON response: %s", r.text[:300])
                return dag_run_id
            return data.get("dag_run_id") or data.get("dagRunId") or dag_run_id

        logger.error("trigger_now: %s %s", r.status_code, r.text[:500])
        return None

    def set_variable(self, key: str, value) -> bool:
        if not self.base or not key:
            return False

        value_str = value if isinstance(value, str) else json.dumps(value)
        var_url = f"{self.base}/api/v1/variables/{key}"
        body = {"key": key, "value": value_str}  # 👈 include key

        try:
            r = self._session.patch(
                var_url,
                headers=self._headers(),
                data=json.dumps(body),
                timeout=self.timeout,
                verify=self.verify_tls,
            )
            if r.status_code in (200, 204):
                return True

            # If not found, create it
            if r.status_code == 404:
                create = self._session.post(
                    f"{self.base}/api/v1/variables",
                    headers=self._headers(),
                    data=json.dumps({"key": key, "value": value_str}),
                    timeout=self.timeout,
                    verify=self.verify_tls,
                )
                return create.status_code in (200, 201)

            # Log details for any other failure (400/401/403/etc.)
            try:
                snippet = r.text[:500]
            except Exception:
                snippet = "<no body>"
            import logging
            logging.getLogger(__name__).error("set_variable PATCH %s -> %s: %s", var_url, r.status_code, snippet)
            return False

        except requests.RequestException:
            import logging
            logging.getLogger(__name__).exception("Error calling Airflow Variables API")
            return False
    
    def get_dag_run_state(self, dag_run_id: str) -> str | None:
        """Return Airflow DAG run state: queued|running|success|failed|..."""
        if not self.base or not self.s.AIRFLOW_DAG_ID:
            return None
        url = f"{self.base}/api/v1/dags/{self.s.AIRFLOW_DAG_ID}/dagRuns/{dag_run_id}"
        r = requests.get(url, headers=self._auth_headers(), timeout=20)
        if r.ok:
            return r.json().get("state")
        return None
