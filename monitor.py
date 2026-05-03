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
       - Deep error extraction (see _job_error for full strategy)
       - LLM analysis
       - Email notification
       - Auto-apply fix  (retry / repair)
       - OR escalation email if LLM says escalate / ignore

ERROR EXTRACTION STRATEGY
═══════════════════════════════════════════════════════════════════════
For JOB failures:
  1. Call GET /api/2.1/jobs/runs/get (fresh call — list API task details
     are often incomplete / missing state_message)
  2. Top-level state.state_message
  3. Per-task state_message for every FAILED task  (most useful)
  4. GET /api/2.1/jobs/runs/get-output → error + error_trace (full
     Python/Scala/SQL stack trace — works for notebook / script tasks)
  5. All four sources are combined so GPT-4o gets the richest possible
     context. Empty sources are skipped.

For PIPELINE failures:
  1. GET /api/2.0/pipelines/{id}/updates/{uid} → cause string
  2. GET /api/2.0/pipelines/{id}/events?filter=level='ERROR'
     → full Spark exception stack traces from error.exceptions[]
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from analyzer import Analyzer, Analysis
from config import Config
from databricks_client import DatabricksClient, DatabricksAPIError
from notifier import Notifier

logger = logging.getLogger(__name__)


# ── TTL-bounded dedup set ─────────────────────────────────────────────────────

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
            # Deep-extract error — pass lightweight run dict as hint only
            error, trace, stdout_logs, notebook_output = self._extract_job_error(run_id, run)
            failures.append(self._job_failure_dict(run, key, error, trace, stdout_logs, notebook_output))
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
            error, trace, stdout_logs, notebook_output = self._extract_job_error(run_id, run)
            failures.append(self._job_failure_dict(run, key, error, trace, stdout_logs, notebook_output))
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

        self._notifier.send_failure_alert(failure, analysis)

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
                failed_tasks = self._failed_task_keys(failure)
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

    def _failed_task_keys(self, failure: Dict) -> List[str]:
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

    # =========================================================================
    # ERROR EXTRACTION — the most important part for LLM quality
    # =========================================================================

    def _extract_job_error(self, run_id: int, hint_run: Dict) -> Tuple[str, str, str, str]:
        """
        Build the richest possible error context for the LLM.

        Strategy (in order):
          1. Fresh GET /api/2.1/jobs/runs/get — the list API's task details
             are often incomplete; a direct get call returns full state_message.
          2. Top-level state.state_message (overall run error summary).
          3. Per-task state_message for every FAILED task — this is usually
             the most specific error (e.g. the actual Python exception).
          4. GET /api/2.1/jobs/runs/get-output — full error_trace (stack trace).
             Only works for notebook / Python script single-task jobs, but when
             it works it gives the exact line number and exception type.

        All non-empty pieces are joined and returned as (error, trace).
        """
        parts: List[str] = []
        trace           = ""
        stdout_logs     = ""
        notebook_output = ""

        # ── Step 1: Fresh run details ─────────────────────────────────────────
        try:
            full_run = self._db.get_run(run_id)
        except DatabricksAPIError as e:
            logger.warning("  Could not fetch run details for %d: %s", run_id, e)
            full_run = hint_run  # fall back to what we already have

        # ── Step 2: Top-level state message ───────────────────────────────────
        top_msg = full_run.get("state", {}).get("state_message", "").strip()
        if top_msg:
            parts.append(f"Run error: {top_msg}")

        # ── Step 3: Per-task errors (most useful for multi-task jobs) ─────────
        tasks = full_run.get("tasks", [])
        failed_tasks = [
            t for t in tasks
            if t.get("state", {}).get("result_state") == "FAILED"
        ]

        if failed_tasks:
            task_errors: List[str] = []
            for t in failed_tasks:
                task_key = t.get("task_key", "unknown_task")
                task_msg = t.get("state", {}).get("state_message", "").strip()
                if task_msg:
                    task_errors.append(f"Task '{task_key}': {task_msg}")
            if task_errors:
                parts.append("Task-level errors:\n" + "\n".join(task_errors))
        elif not tasks:
            parts.append("(No per-task detail available — single-task or classic job)")

        # ── Step 4: get_run_output — stderr (error_trace) + stdout (logs) ─────
        # This is the richest source:
        #   error       = short error message
        #   error_trace = full Python / Scala / SQL stack trace (stderr)
        #   logs        = everything printed to stdout during the run
        #   notebook_output = cell outputs for notebook tasks
        try:
            output = self._db.get_run_output(run_id)

            out_error   = output.get("error", "").strip()
            out_trace   = output.get("error_trace", "").strip()
            out_logs    = output.get("logs", "").strip()          # stdout
            out_nb      = ""
            nb_result   = output.get("notebook_output", {})
            if isinstance(nb_result, dict):
                out_nb = nb_result.get("result", "").strip()

            if out_error and out_error not in top_msg:
                parts.append(f"Output error: {out_error}")
            if out_trace:
                trace = out_trace
            if out_logs:
                stdout_logs = out_logs
            if out_nb:
                notebook_output = out_nb

        except DatabricksAPIError:
            # get-output is not available for all task types — expected for
            # multi-task jobs; per-task state_message (Step 3) covers those.
            pass

        # ── Step 5: For multi-task jobs, try get_run_output per failed task ───
        # Each task in a multi-task job has its own run_id (attempt_number=0).
        # Calling get_run_output with the task's run_id retrieves task-level
        # stdout/stderr which is often unavailable on the parent run.
        if failed_tasks and not trace and not stdout_logs:
            task_outputs: List[str] = []
            task_stdouts: List[str] = []
            for t in failed_tasks[:3]:  # limit to first 3 failed tasks
                task_run_id = t.get("run_id")
                task_key    = t.get("task_key", "unknown")
                if not task_run_id:
                    continue
                try:
                    tout = self._db.get_run_output(int(task_run_id))
                    t_trace  = tout.get("error_trace", "").strip()
                    t_logs   = tout.get("logs", "").strip()
                    t_error  = tout.get("error", "").strip()
                    t_nb     = ""
                    t_nb_res = tout.get("notebook_output", {})
                    if isinstance(t_nb_res, dict):
                        t_nb = t_nb_res.get("result", "").strip()

                    if t_trace:
                        task_outputs.append(f"[task: {task_key}] stderr:\n{t_trace}")
                    elif t_error:
                        task_outputs.append(f"[task: {task_key}] error: {t_error}")
                    if t_logs:
                        task_stdouts.append(f"[task: {task_key}] stdout:\n{t_logs}")
                    if t_nb:
                        task_stdouts.append(f"[task: {task_key}] notebook output:\n{t_nb}")
                except DatabricksAPIError:
                    pass

            if task_outputs:
                trace = "\n\n".join(task_outputs)
            if task_stdouts:
                stdout_logs = "\n\n".join(task_stdouts)

        error = "\n\n".join(parts)

        # ── Diagnostic log ────────────────────────────────────────────────────
        logger.info(
            "  Extracted for run %d | error=%d chars | stderr=%d chars | stdout=%d chars | tasks_failed=%d",
            run_id, len(error), len(trace), len(stdout_logs), len(failed_tasks),
        )
        if not error and not trace and not stdout_logs:
            logger.warning(
                "  ⚠️  All log sources empty for run %d. "
                "Verify the Databricks token has CAN_VIEW permission on the job.",
                run_id,
            )

        return error, trace, stdout_logs, notebook_output

    def _pipeline_cause(self, pid: str, update_id: str) -> str:
        try:
            return self._db.get_pipeline_update(pid, update_id).get("update", {}).get("cause", "")
        except DatabricksAPIError:
            return ""

    def _pipeline_events(self, pid: str) -> str:
        """
        Fetch ERROR-level pipeline events.
        error.exceptions[] contains the full Spark stack trace — far richer
        than the brief cause string in the update record.
        """
        try:
            events = self._db.list_pipeline_events(pid, max_results=10)
            lines = []
            for e in events:
                msg = e.get("message", "").strip()
                excs = e.get("error", {}).get("exceptions", [])
                exc_text = "\n".join(
                    x.get("message", "").strip()
                    for x in excs if x.get("message", "").strip()
                )
                combined = "\n".join(filter(None, [msg, exc_text]))
                if combined:
                    lines.append(combined)
            result = "\n\n".join(lines)
            if result:
                logger.info("  Pipeline events extracted for %s | len=%d", pid, len(result))
            else:
                logger.warning("  ⚠️  No pipeline error events found for %s", pid)
            return result
        except DatabricksAPIError as e:
            logger.warning("  Pipeline events fetch failed for %s: %s", pid, e)
            return ""

    @staticmethod
    def _job_failure_dict(run: Dict, key: str, error: str, trace: str,
                          stdout_logs: str = "", notebook_output: str = "") -> Dict:
        return {
            "type": "job",
            "job_id": int(run["job_id"]),
            "run_id": int(run["run_id"]),
            "name": run.get("run_name", str(run["job_id"])),
            "failure_key": key,
            "error": error,
            "error_trace": trace,
            "stdout_logs": stdout_logs,
            "notebook_output": notebook_output,
            "run_url": run.get("run_page_url", ""),
            "_run_data": run,
        }
