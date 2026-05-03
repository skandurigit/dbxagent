"""
monitor.py — 24/7 monitoring of ALL Databricks jobs and pipelines.

Flow per cycle:
  1. Strategy A  — scan all completed runs since last poll
                   GET /api/2.1/jobs/runs/list?completed_only=true&start_time_from=<ms>
  2. Strategy B  — detect runs that transitioned from active → failed
                   GET /api/2.1/jobs/runs/list?active_only=true  (diff with last cycle)
  3. Pipelines   — scan all pipeline states
                   GET /api/2.0/pipelines  (paginated)
  4. For each failure:
       - LLM analysis
       - Email notification
       - Auto-apply fix  (retry / repair)
       - OR escalation email if LLM says escalate / ignore
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from analyzer import Analyzer, Analysis
from config import Config
from databricks_client import DatabricksClient, DatabricksAPIError
from notifier import Notifier

logger = logging.getLogger(__name__)


class _TTLSet:
    def __init__(self, ttl: int = 7200) -> None:
        self._ttl = ttl
        self._store: Dict[str, float] = {}

    def add(self, key: str) -> None:
        self._store[key] = time.monotonic() + self._ttl

    def __contains__(self, key: object) -> bool:
        exp = self._store.get(key)  # type: ignore[arg-type]
        if exp is None:
            return False
        if time.monotonic() > exp:
            del self._store[key]  # type: ignore[arg-type]
            return False
        return True

    def purge(self) -> None:
        now = time.monotonic()
        self._store = {k: v for k, v in self._store.items() if v > now}


@dataclass
class _RetryState:
    count: int = 0
    last_at: float = field(default_factory=time.monotonic)


class Monitor:

    def __init__(
        self,
        cfg: Config,
        db: DatabricksClient,
        analyzer: Analyzer,
        notifier: Notifier,
    ) -> None:
        self._cfg = cfg
        self._db = db
        self._analyzer = analyzer
        self._notifier = notifier
        self._seen = _TTLSet(ttl=7200)
        self._retries: Dict[str, _RetryState] = {}
        self._prev_active: Dict[int, Dict] = {}
        self._first_cycle = True
        lookback_ms = cfg.lookback_minutes * 60 * 1000
        self._last_poll_ms: int = int(time.time() * 1000) - lookback_ms

    # ── Main cycle ────────────────────────────────────────────────────────────

    def run_cycle(self) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        logger.info("─── Scan cycle @ %s ───────────────────────────────", ts)

        job_a      = self._strategy_a_completed_runs()
        job_b      = self._strategy_b_active_transitions()
        pipelines  = self._scan_pipelines()
        all_new    = job_a + job_b + pipelines

        logger.info(
            "  Jobs (completed): %d  |  Jobs (transitions): %d  |  Pipelines: %d  |  Total: %d",
            len(job_a), len(job_b), len(pipelines), len(all_new),
        )

        for failure in all_new:
            self._handle_failure(failure)

        self._last_poll_ms = int(time.time() * 1000)
        self._first_cycle = False
        self._seen.purge()

    # ── Strategy A ────────────────────────────────────────────────────────────

    def _strategy_a_completed_runs(self) -> List[Dict]:
        failures = []
        try:
            runs = self._db.list_runs_completed(start_time_from_ms=self._last_poll_ms)
        except DatabricksAPIError as e:
            logger.error("Strategy A API error: %s", e)
            return []

        for run in runs:
            if run.get("state", {}).get("result_state") != "FAILED":
                continue
            run_id = int(run["run_id"])
            job_id = int(run["job_id"])
            key = f"job_{job_id}_{run_id}"
            if key in self._seen:
                continue
            self._seen.add(key)
            error, trace = self._job_error(run_id, run)
            failures.append(self._job_failure_dict(run, key, error, trace))
            logger.warning("  [A] FAILED | job=%s run=%d", run.get("run_name", job_id), run_id)

        return failures

    # ── Strategy B ────────────────────────────────────────────────────────────

    def _strategy_b_active_transitions(self) -> List[Dict]:
        failures = []
        try:
            active_now = {int(r["run_id"]): r for r in self._db.list_runs_active()}
        except DatabricksAPIError as e:
            logger.error("Strategy B API error: %s", e)
            return []

        if self._first_cycle:
            self._prev_active = active_now
            logger.debug("  [B] Initialised active tracker: %d runs", len(active_now))
            return []

        vanished: Set[int] = set(self._prev_active.keys()) - set(active_now.keys())
        for run_id in vanished:
            try:
                run = self._db.get_run(run_id)
            except DatabricksAPIError:
                continue
            if run.get("state", {}).get("result_state") != "FAILED":
                continue
            job_id = int(run.get("job_id", 0))
            key = f"job_{job_id}_{run_id}"
            if key in self._seen:
                continue
            self._seen.add(key)
            error, trace = self._job_error(run_id, run)
            failures.append(self._job_failure_dict(run, key, error, trace))
            logger.warning("  [B] Transition FAILED | job=%s run=%d", run.get("run_name", job_id), run_id)

        self._prev_active = active_now
        return failures

    # ── Pipelines ─────────────────────────────────────────────────────────────

    def _scan_pipelines(self) -> List[Dict]:
        failures = []
        try:
            pipelines = self._db.list_pipelines_all()
        except DatabricksAPIError as e:
            logger.error("Pipeline scan API error: %s", e)
            return []

        for p in pipelines:
            pid   = p.get("pipeline_id", "")
            name  = p.get("name", pid)
            updates = p.get("latest_updates", [])
            if not updates or updates[0].get("state") != "FAILED":
                continue
            update_id = updates[0].get("update_id", "")
            key = f"pipeline_{pid}_{update_id}"
            if key in self._seen:
                continue
            self._seen.add(key)
            cause  = self._pipeline_cause(pid, update_id)
            events = self._pipeline_events(pid)
            failures.append({
                "type": "pipeline",
                "pipeline_id": pid,
                "update_id": update_id,
                "name": name,
                "failure_key": key,
                "error": cause,
                "error_trace": events,
                "run_url": f"{self._cfg.databricks_host}/#joblist/pipelines/{pid}",
            })
            logger.warning("  [P] FAILED | name=%s update=%s", name, update_id)

        return failures

    # ── Failure handler ───────────────────────────────────────────────────────

    def _handle_failure(self, failure: Dict) -> None:
        analysis = self._analyzer.analyze(failure)

        logger.info(
            "  Analysis | %s\n"
            "    Action     : %s\n"
            "    Confidence : %s\n"
            "    Root Cause : %s\n"
            "    Reasoning  : %s\n"
            "    Fix Hint   : %s",
            failure["failure_key"],
            analysis.recommended_action,
            analysis.confidence,
            analysis.root_cause,
            analysis.reasoning,
            analysis.config_changes or "none",
        )

        if analysis.recommended_action in ("escalate", "ignore"):
            self._notifier.send_escalation(failure, analysis)
            return

        # Send failure alert email
        self._notifier.send_failure_alert(failure, analysis)

        # Auto-apply fix
        action_taken = self._apply_fix(failure, analysis)
        if action_taken:
            self._notifier.send_fix_applied(failure, analysis, action_taken)
            logger.info("  Fix applied | %s → %s", failure["failure_key"], action_taken)

    # ── Remediation ───────────────────────────────────────────────────────────

    def _apply_fix(self, failure: Dict, analysis: Analysis) -> Optional[str]:
        key   = failure["failure_key"]
        ftype = failure["type"]
        state = self._retries.setdefault(key, _RetryState())

        if state.count >= self._cfg.max_retries:
            logger.warning("Max retries reached for %s", key)
            return None

        backoff_sec = (self._cfg.retry_backoff_base_min ** state.count) * 60
        if state.count > 0 and (time.monotonic() - state.last_at) < backoff_sec:
            return None

        try:
            if analysis.recommended_action == "retry":
                if ftype == "pipeline":
                    self._db.start_pipeline_update(failure["pipeline_id"])
                else:
                    self._db.run_now(int(failure["job_id"]))
                state.count += 1
                state.last_at = time.monotonic()
                return f"retry attempt #{state.count}"

            elif analysis.recommended_action == "repair" and ftype == "job":
                failed_tasks = self._failed_tasks(failure)
                if failed_tasks:
                    self._db.repair_run(int(failure["run_id"]), failed_tasks)
                    return f"repair — tasks: {', '.join(failed_tasks)}"
                else:
                    self._db.run_now(int(failure["job_id"]))
                    state.count += 1
                    state.last_at = time.monotonic()
                    return f"retry attempt #{state.count} (no specific tasks found)"

        except DatabricksAPIError as e:
            logger.error("Fix failed for %s: %s", key, e)

        return None

    def _failed_tasks(self, failure: Dict) -> List[str]:
        run_data = failure.get("_run_data") or {}
        tasks = run_data.get("tasks", [])
        if not tasks:
            try:
                run_data = self._db.get_run(int(failure["run_id"]))
                tasks = run_data.get("tasks", [])
            except DatabricksAPIError:
                pass
        return [
            t["task_key"] for t in tasks
            if t.get("state", {}).get("result_state") == "FAILED" and t.get("task_key")
        ]

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _job_error(self, run_id: int, run: Dict):
        error = run.get("state", {}).get("state_message", "")
        tasks = run.get("tasks", [])
        if tasks:
            msgs = [
                f"Task '{t.get('task_key')}': {t.get('state', {}).get('state_message', '')}"
                for t in tasks if t.get("state", {}).get("result_state") == "FAILED"
            ]
            if msgs:
                error = "\n".join(msgs)
        trace = ""
        try:
            out = self._db.get_run_output(run_id)
            trace = out.get("error_trace", "")
            if not error:
                error = out.get("error", "")
        except DatabricksAPIError:
            pass
        return error, trace

    def _pipeline_cause(self, pid: str, update_id: str) -> str:
        try:
            return self._db.get_pipeline_update(pid, update_id).get("update", {}).get("cause", "")
        except DatabricksAPIError:
            return ""

    def _pipeline_events(self, pid: str) -> str:
        try:
            events = self._db.list_pipeline_events(pid, max_results=10)
            lines = []
            for e in events:
                msg = e.get("message", "")
                excs = e.get("error", {}).get("exceptions", [])
                exc_text = "\n".join(x.get("message", "") for x in excs if x.get("message"))
                lines.append(f"{msg}\n{exc_text}".strip())
            return "\n\n".join(lines)
        except DatabricksAPIError:
            return ""

    @staticmethod
    def _job_failure_dict(run: Dict, key: str, error: str, trace: str) -> Dict:
        return {
            "type": "job",
            "job_id": int(run["job_id"]),
            "run_id": int(run["run_id"]),
            "name": run.get("run_name", str(run["job_id"])),
            "failure_key": key,
            "error": error,
            "error_trace": trace,
            "run_url": run.get("run_page_url", ""),
            "_run_data": run,
        }
