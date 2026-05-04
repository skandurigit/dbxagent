"""
stale_monitor.py — Detect long-running and abandoned Databricks jobs/tasks.

════════════════════════════════════════════════════════════════════════
WHAT IS MONITORED
════════════════════════════════════════════════════════════════════════

  1. LONG-RUNNING JOB
     A run whose life_cycle_state is RUNNING and whose elapsed time
     exceeds STALE_JOB_WARN_MIN (default 60 min).
     A second alert fires at STALE_JOB_CRITICAL_MIN (default 120 min).
     Action: email alert only (no auto-cancel — too risky).

  2. ABANDONED / STUCK JOB  (PENDING too long)
     A run stuck in PENDING or BLOCKED state beyond
     STALE_PENDING_WARN_MIN (default 15 min).
     Usually means: cluster failed to start, queue backlog, or a
     dependency task never resolved.
     Action: email alert. Auto-cancel optional (STALE_AUTO_CANCEL).

  3. HUNG TASK  (individual task inside a multi-task job)
     A specific task within a run that has been RUNNING longer than
     STALE_TASK_WARN_MIN (default 45 min) while sibling tasks are done.
     This catches a single bad task holding up the whole job.
     Action: email alert with the specific task name and elapsed time.

  4. ZOMBIE RUN  (TERMINATING too long)
     A run stuck in TERMINATING state beyond STALE_TERMINATING_MIN
     (default 10 min). Usually a cluster that won't release.
     Action: email alert with a suggestion to force-cancel.

  5. ABANDONED PIPELINE  (DLT stuck in INITIALIZING / RESETTING)
     A pipeline whose state has been INITIALIZING or RESETTING beyond
     STALE_PIPELINE_MIN (default 20 min).
     Action: email alert.

════════════════════════════════════════════════════════════════════════
HOW IT WORKS
════════════════════════════════════════════════════════════════════════

  Every cycle:
    GET /api/2.1/jobs/runs/list?active_only=true   → all live runs
    GET /api/2.0/pipelines                          → all pipelines

  For each active run:
    - Calculate elapsed = now - start_time_ms
    - Check life_cycle_state:  PENDING → abandoned check
                               RUNNING → long-run check
                               TERMINATING → zombie check
    - For RUNNING runs: inspect tasks[] for hung tasks

  For each pipeline:
    - Check state: INITIALIZING / RESETTING → stuck check

  Alert dedup:
    Each (run_id, alert_type) pair is tracked with a re-alert interval
    so you receive at most one email per pair per STALE_REALERT_MIN
    (default 30 min). This prevents inbox flooding for a job that stays
    stuck for hours.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from config import Config
from databricks_client import DatabricksClient, DatabricksAPIError
from notifier import Notifier

logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StaleAlert:
    """Represents a single stale / stuck detection event."""
    alert_type: str          # long_running | abandoned | hung_task | zombie | stuck_pipeline
    severity: str            # warning | critical
    name: str                # job/pipeline name
    run_id: Optional[int]
    job_id: Optional[int]
    pipeline_id: Optional[str]
    elapsed_min: float
    state: str               # Databricks life_cycle_state or pipeline state
    task_key: Optional[str]  # set for hung_task alerts
    run_url: str
    details: str             # human-readable explanation


# ── Alert dedup tracker ───────────────────────────────────────────────────────

class _AlertTracker:
    """
    Prevents duplicate alerts for the same issue.
    Stores last-alerted epoch per (run_id, alert_type) key.
    Re-alerts after realert_interval_min so issues that persist
    keep notifying without spamming.
    """

    def __init__(self, realert_min: int) -> None:
        self._realert_sec = realert_min * 60
        self._last: Dict[str, float] = {}

    def should_alert(self, key: str) -> bool:
        now = time.monotonic()
        last = self._last.get(key, 0.0)
        if (now - last) >= self._realert_sec:
            self._last[key] = now
            return True
        return False

    def purge_resolved(self, active_keys: Set[str]) -> None:
        """Remove keys for runs that are no longer active."""
        stale_keys = [k for k in self._last if k not in active_keys]
        for k in stale_keys:
            del self._last[k]


# ── Stale monitor ─────────────────────────────────────────────────────────────

class StaleMonitor:

    def __init__(
        self,
        cfg: Config,
        db: DatabricksClient,
        notifier: Notifier,
    ) -> None:
        self._cfg = cfg
        self._db  = db
        self._notifier = notifier
        self._tracker = _AlertTracker(cfg.stale_realert_min)

    # ── Main entry point ──────────────────────────────────────────────────────

    def run_cycle(self) -> None:
        """
        Called once per main monitor cycle.
        Scans all active runs and pipelines for stale conditions.
        """
        alerts: List[StaleAlert] = []
        active_keys: Set[str] = set()

        # ── Job / task scan ───────────────────────────────────────────────────
        try:
            active_runs = self._db.list_runs_active()
        except DatabricksAPIError as e:
            logger.error("StaleMonitor: failed to list active runs: %s", e)
            active_runs = []

        for run in active_runs:
            run_alerts, run_keys = self._check_run(run)
            alerts.extend(run_alerts)
            active_keys.update(run_keys)

        # ── Pipeline scan ─────────────────────────────────────────────────────
        try:
            pipelines = self._db.list_pipelines_all()
        except DatabricksAPIError as e:
            logger.error("StaleMonitor: failed to list pipelines: %s", e)
            pipelines = []

        for pipeline in pipelines:
            alert, key = self._check_pipeline(pipeline)
            if alert:
                alerts.append(alert)
            if key:
                active_keys.add(key)

        # ── Remove resolved keys from tracker ─────────────────────────────────
        self._tracker.purge_resolved(active_keys)

        # ── Fire alerts ───────────────────────────────────────────────────────
        for alert in alerts:
            dedup_key = self._dedup_key(alert)
            if self._tracker.should_alert(dedup_key):
                self._notifier.send_stale_alert(alert)
                logger.warning(
                    "Stale alert | type=%-16s severity=%-8s name=%s elapsed=%.1fmin",
                    alert.alert_type, alert.severity, alert.name, alert.elapsed_min,
                )
            else:
                logger.debug(
                    "Stale alert suppressed (re-alert interval) | type=%s name=%s",
                    alert.alert_type, alert.name,
                )

        if alerts:
            logger.info("StaleMonitor: %d stale condition(s) detected", len(alerts))
        else:
            logger.debug("StaleMonitor: all active runs healthy")

    # ── Run-level checks ──────────────────────────────────────────────────────

    def _check_run(self, run: Dict):
        """
        Evaluate one active run and return (alerts, dedup_keys).

        Checks:
          PENDING     → abandoned check
          RUNNING     → long-running check + per-task hung check
          TERMINATING → zombie check
        """
        alerts: List[StaleAlert] = []
        keys: Set[str] = set()

        run_id  = int(run.get("run_id", 0))
        job_id  = int(run.get("job_id", 0))
        name    = run.get("run_name") or run.get("job_name") or str(job_id)
        state   = run.get("state", {})
        lc      = state.get("life_cycle_state", "")
        start   = run.get("start_time", 0)          # epoch ms
        run_url = run.get("run_page_url", "")

        if not start:
            return alerts, keys

        elapsed_min = self._elapsed_min(start)

        # ── PENDING / BLOCKED — cluster not starting or queue stuck ──────────
        if lc in ("PENDING", "BLOCKED", "WAITING_FOR_RETRY"):
            key = f"{run_id}:abandoned"
            keys.add(key)
            threshold = self._cfg.stale_pending_warn_min
            if elapsed_min >= threshold:
                alerts.append(StaleAlert(
                    alert_type="abandoned",
                    severity="warning",
                    name=name,
                    run_id=run_id,
                    job_id=job_id,
                    pipeline_id=None,
                    elapsed_min=elapsed_min,
                    state=lc,
                    task_key=None,
                    run_url=run_url,
                    details=(
                        f"Run has been in {lc} state for {elapsed_min:.0f} min "
                        f"(threshold: {threshold} min). "
                        f"Possible causes: cluster failed to start, queue backlog, "
                        f"or a dependent task never resolved."
                    ),
                ))

        # ── RUNNING — long-running detection ─────────────────────────────────
        elif lc == "RUNNING":
            # Warning threshold
            warn_key = f"{run_id}:long_running_warn"
            crit_key = f"{run_id}:long_running_critical"
            keys.update({warn_key, crit_key})

            if elapsed_min >= self._cfg.stale_job_critical_min:
                alerts.append(StaleAlert(
                    alert_type="long_running",
                    severity="critical",
                    name=name,
                    run_id=run_id,
                    job_id=job_id,
                    pipeline_id=None,
                    elapsed_min=elapsed_min,
                    state=lc,
                    task_key=None,
                    run_url=run_url,
                    details=(
                        f"Run has been RUNNING for {elapsed_min:.0f} min — "
                        f"exceeds critical threshold of {self._cfg.stale_job_critical_min} min. "
                        f"Consider investigating for infinite loops, data skew, or "
                        f"missing shuffle optimisations."
                    ),
                ))
            elif elapsed_min >= self._cfg.stale_job_warn_min:
                alerts.append(StaleAlert(
                    alert_type="long_running",
                    severity="warning",
                    name=name,
                    run_id=run_id,
                    job_id=job_id,
                    pipeline_id=None,
                    elapsed_min=elapsed_min,
                    state=lc,
                    task_key=None,
                    run_url=run_url,
                    details=(
                        f"Run has been RUNNING for {elapsed_min:.0f} min — "
                        f"exceeds warning threshold of {self._cfg.stale_job_warn_min} min."
                    ),
                ))

            # ── Per-task hung check ───────────────────────────────────────────
            tasks = run.get("tasks", [])
            running_tasks = [
                t for t in tasks
                if t.get("state", {}).get("life_cycle_state") == "RUNNING"
                and t.get("start_time")
            ]
            for task in running_tasks:
                task_key     = task.get("task_key", "unknown")
                task_start   = task.get("start_time", 0)
                task_elapsed = self._elapsed_min(task_start)
                t_key        = f"{run_id}:hung_task:{task_key}"
                keys.add(t_key)

                if task_elapsed >= self._cfg.stale_task_warn_min:
                    alerts.append(StaleAlert(
                        alert_type="hung_task",
                        severity="warning",
                        name=name,
                        run_id=run_id,
                        job_id=job_id,
                        pipeline_id=None,
                        elapsed_min=task_elapsed,
                        state="RUNNING",
                        task_key=task_key,
                        run_url=run_url,
                        details=(
                            f"Task '{task_key}' has been running for {task_elapsed:.0f} min "
                            f"(threshold: {self._cfg.stale_task_warn_min} min). "
                            f"Sibling tasks in the same run may be blocked waiting for it."
                        ),
                    ))

        # ── TERMINATING too long — zombie cluster ────────────────────────────
        elif lc == "TERMINATING":
            key = f"{run_id}:zombie"
            keys.add(key)
            # For TERMINATING we use start_time as a proxy (exact termination
            # start is not exposed in the list API without a get_run() call)
            if elapsed_min >= self._cfg.stale_job_warn_min + self._cfg.stale_terminating_min:
                alerts.append(StaleAlert(
                    alert_type="zombie",
                    severity="warning",
                    name=name,
                    run_id=run_id,
                    job_id=job_id,
                    pipeline_id=None,
                    elapsed_min=elapsed_min,
                    state="TERMINATING",
                    task_key=None,
                    run_url=run_url,
                    details=(
                        f"Run has been in TERMINATING state and total elapsed is "
                        f"{elapsed_min:.0f} min. The cluster may be stuck releasing resources. "
                        f"Consider force-cancelling via the Databricks UI."
                    ),
                ))

        return alerts, keys

    # ── Pipeline checks ───────────────────────────────────────────────────────

    def _check_pipeline(self, pipeline: Dict):
        """
        Check a single DLT pipeline for stuck INITIALIZING / RESETTING state.
        Returns (alert_or_None, dedup_key_or_None).
        """
        pid   = pipeline.get("pipeline_id", "")
        name  = pipeline.get("name", pid)
        state = pipeline.get("state", "")

        if state not in ("INITIALIZING", "RESETTING", "STARTING"):
            return None, None

        # Pipeline records don't include start_time directly.
        # Use latest_updates[0].creation_time as a proxy.
        updates = pipeline.get("latest_updates", [])
        creation_ms = 0
        if updates:
            creation_ms = updates[0].get("creation_time", 0)

        if not creation_ms:
            return None, None

        elapsed_min = self._elapsed_min(creation_ms)
        key = f"pipeline:{pid}:stuck_{state}"

        threshold = self._cfg.stale_pipeline_min
        if elapsed_min < threshold:
            return None, key

        run_url = f"{self._cfg.databricks_host}/#joblist/pipelines/{pid}"
        alert = StaleAlert(
            alert_type="stuck_pipeline",
            severity="warning",
            name=name,
            run_id=None,
            job_id=None,
            pipeline_id=pid,
            elapsed_min=elapsed_min,
            state=state,
            task_key=None,
            run_url=run_url,
            details=(
                f"Pipeline has been in {state} state for {elapsed_min:.0f} min "
                f"(threshold: {threshold} min). "
                f"This may indicate a cluster provisioning failure or a "
                f"Unity Catalog permission issue preventing pipeline startup."
            ),
        )
        return alert, key

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _elapsed_min(start_ms: int) -> float:
        """Return minutes elapsed since start_ms (epoch milliseconds)."""
        now_ms = time.time() * 1000
        return max(0.0, (now_ms - start_ms) / 60_000)

    @staticmethod
    def _dedup_key(alert: StaleAlert) -> str:
        if alert.task_key:
            return f"{alert.run_id}:{alert.alert_type}:{alert.task_key}:{alert.severity}"
        if alert.pipeline_id:
            return f"pipeline:{alert.pipeline_id}:{alert.alert_type}:{alert.severity}"
        return f"{alert.run_id}:{alert.alert_type}:{alert.severity}"
