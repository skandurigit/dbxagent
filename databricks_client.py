"""
databricks_client.py — Databricks REST API client.

Endpoints used by this agent:

  JOBS API (v2.1)
  ───────────────────────────────────────────────────────────
  GET  /api/2.1/jobs/runs/list  ?completed_only  → list_runs_completed()
  GET  /api/2.1/jobs/runs/list  ?active_only     → list_runs_active()
  GET  /api/2.1/jobs/runs/get                    → get_run()
  GET  /api/2.1/jobs/runs/get-output             → get_run_output()
  POST /api/2.1/jobs/run-now                     → run_now()
  POST /api/2.1/jobs/runs/repair                 → repair_run()

  PIPELINES (DLT) API (v2.0)
  ───────────────────────────────────────────────────────────
  GET  /api/2.0/pipelines              (paginated) → list_pipelines_all()
  GET  /api/2.0/pipelines/{id}                    → get_pipeline()
  GET  /api/2.0/pipelines/{id}/updates/{uid}      → get_pipeline_update()
  GET  /api/2.0/pipelines/{id}/events             → list_pipeline_events()
  POST /api/2.0/pipelines/{id}/updates            → start_pipeline_update()
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Generator, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)


class DatabricksAPIError(Exception):
    def __init__(self, status: int, body: str, url: str = "") -> None:
        self.status = status
        super().__init__(f"HTTP {status} @ {url}: {body[:300]}")


class DatabricksClient:

    def __init__(self, host: str, token: str, timeout: int = 30) -> None:
        self._host = host
        self._timeout = timeout
        self._s = requests.Session()
        self._s.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        adapter = HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        ))
        self._s.mount("https://", adapter)
        self._s.mount("http://", adapter)

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self._host}{path}"
        r = self._s.get(url, params=params, timeout=self._timeout)
        if not r.ok:
            raise DatabricksAPIError(r.status_code, r.text, url)
        return r.json()

    def _post(self, path: str, body: Optional[Dict] = None) -> Dict:
        url = f"{self._host}{path}"
        r = self._s.post(url, json=body or {}, timeout=self._timeout)
        if not r.ok:
            raise DatabricksAPIError(r.status_code, r.text, url)
        return r.json()

    # =========================================================================
    # JOBS API — Run listing
    # =========================================================================

    def list_runs_completed(
        self,
        start_time_from_ms: Optional[int] = None,
        page_size: int = 25,
        max_results: int = 2000,
    ) -> List[Dict]:
        """
        GET /api/2.1/jobs/runs/list?completed_only=true[&start_time_from=<ms>]

        Returns all completed runs across EVERY job in the workspace.

        start_time_from_ms: only return runs that STARTED at or after this
        epoch-millisecond timestamp.  The monitor passes its last-poll time
        here so each cycle fetches only the new window — not the entire history.

        expand_tasks=true is included so per-task failure state is inline,
        avoiding a separate get_run() call to find which tasks failed.
        """
        extra: Dict[str, Any] = {"completed_only": "true"}
        if start_time_from_ms:
            extra["start_time_from"] = start_time_from_ms
        return list(self._paginate_runs(extra, page_size, max_results))

    def list_runs_active(self, page_size: int = 25) -> List[Dict]:
        """
        GET /api/2.1/jobs/runs/list?active_only=true

        Returns every run currently in state PENDING / RUNNING / TERMINATING
        across all jobs in the workspace.

        The monitor calls this every cycle and diffs the result against the
        previous cycle's snapshot.  Any run_id that was present last cycle but
        absent this cycle has just terminated — we then call get_run() to learn
        whether it ended in FAILED.

        This catches jobs that were already running when the agent started
        (Strategy A's start_time_from would miss those).
        """
        return list(self._paginate_runs({"active_only": "true"}, page_size, 10_000))

    def _paginate_runs(
        self,
        extra: Dict[str, Any],
        page_size: int,
        max_results: int,
    ) -> Generator[Dict, None, None]:
        token: Optional[str] = None
        total = 0
        while total < max_results:
            params: Dict[str, Any] = {
                "limit": min(page_size, max_results - total),
                "expand_tasks": True,
                **extra,
            }
            if token:
                params["page_token"] = token
            resp = self._get("/api/2.1/jobs/runs/list", params)
            for run in resp.get("runs", []):
                yield run
                total += 1
                if total >= max_results:
                    return
            token = resp.get("next_page_token")
            if not token or not resp.get("has_more", False):
                break

    def get_run(self, run_id: int) -> Dict:
        """
        GET /api/2.1/jobs/runs/get?run_id=<id>

        Full run record: state, per-task states, cluster info, run_page_url.
        Called when a run transitions out of active state to check final outcome.
        """
        return self._get("/api/2.1/jobs/runs/get",
                         {"run_id": run_id, "expand_tasks": True})

    def get_run_output(self, run_id: int) -> Dict:
        """
        GET /api/2.1/jobs/runs/get-output?run_id=<id>

        Returns error and error_trace (full Python/Scala/SQL stack trace).
        Available for notebook / script tasks.  For multi-task jobs the
        per-task state_message from get_run() is used instead.
        """
        return self._get("/api/2.1/jobs/runs/get-output", {"run_id": run_id})

    def run_now(self, job_id: int) -> Dict:
        """POST /api/2.1/jobs/run-now — trigger an immediate job run."""
        return self._post("/api/2.1/jobs/run-now", {"job_id": job_id})

    def repair_run(self, run_id: int, rerun_tasks: List[str]) -> Dict:
        """
        POST /api/2.1/jobs/runs/repair

        Re-runs only the failed tasks, preserving outputs from succeeded tasks.
        rerun_tasks must contain valid, non-None task_key strings.
        """
        if not rerun_tasks:
            raise ValueError("rerun_tasks cannot be empty")
        return self._post("/api/2.1/jobs/runs/repair",
                          {"run_id": run_id, "rerun_tasks": rerun_tasks})

    # =========================================================================
    # PIPELINES (DLT) API
    # =========================================================================

    def list_pipelines_all(self, page_size: int = 100) -> List[Dict]:
        """
        GET /api/2.0/pipelines  (paginated via next_page_token)

        Returns every DLT pipeline in the workspace.
        Each record includes: pipeline_id, name, state, latest_updates[].

        The monitor iterates this list every cycle.  `state` == "FAILED" and
        latest_updates[0].state == "FAILED" are the two triggers for
        pipeline failure detection — no per-pipeline polling needed.
        """
        pipelines: List[Dict] = []
        token: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"max_results": page_size}
            if token:
                params["page_token"] = token
            resp = self._get("/api/2.0/pipelines", params)
            pipelines.extend(resp.get("statuses", []))
            token = resp.get("next_page_token")
            if not token:
                break
        logger.debug("list_pipelines_all → %d pipelines", len(pipelines))
        return pipelines

    def get_pipeline(self, pipeline_id: str) -> Dict:
        """GET /api/2.0/pipelines/{id} — full spec + live state."""
        return self._get(f"/api/2.0/pipelines/{pipeline_id}")

    def get_pipeline_update(self, pipeline_id: str, update_id: str) -> Dict:
        """
        GET /api/2.0/pipelines/{id}/updates/{uid}

        Returns cause field — a short description of why the update failed.
        """
        return self._get(f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")

    def list_pipeline_events(self, pipeline_id: str, max_results: int = 20) -> List[Dict]:
        """
        GET /api/2.0/pipelines/{id}/events?filter=level='ERROR'

        Returns ERROR-level events for the pipeline.  Each event contains
        error.exceptions[] with the full Spark stack trace — far more detail
        than the brief `cause` string in the update record.

        This is the richest source of root-cause information for DLT failures.
        """
        events: List[Dict] = []
        token: Optional[str] = None
        while len(events) < max_results:
            params: Dict[str, Any] = {
                "max_results": min(100, max_results - len(events)),
                "filter": "level='ERROR'",
            }
            if token:
                params["page_token"] = token
            resp = self._get(f"/api/2.0/pipelines/{pipeline_id}/events", params)
            batch = resp.get("events", [])
            events.extend(batch)
            token = resp.get("next_page_token")
            if not token or not batch:
                break
        return events

    def start_pipeline_update(self, pipeline_id: str, full_refresh: bool = False) -> Dict:
        """POST /api/2.0/pipelines/{id}/updates — trigger a new pipeline run."""
        return self._post(
            f"/api/2.0/pipelines/{pipeline_id}/updates",
            {"full_refresh": full_refresh},
        )
