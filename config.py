"""config.py — All settings loaded from environment variables."""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import List


def _csv(key: str, default: str = "") -> List[str]:
    return [x.strip() for x in os.getenv(key, default).split(",") if x.strip()]


@dataclass
class Config:
    # ── Required ──────────────────────────────────────────────────────────────
    databricks_host: str
    databricks_token: str
    openai_api_key: str

    # ── LLM ───────────────────────────────────────────────────────────────────
    openai_model: str = "gpt-4o"

    # ── Monitoring ────────────────────────────────────────────────────────────
    poll_interval_sec: int = 60
    lookback_minutes: int = 60

    # ── Remediation ───────────────────────────────────────────────────────────
    max_retries: int = 3
    retry_backoff_base_min: float = 2.0

    # ── Email (SMTP) ──────────────────────────────────────────────────────────
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_tls: bool = True
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    email_to: List[str] = field(default_factory=list)

    # ── Misc ──────────────────────────────────────────────────────────────────
    log_file: str = "/var/log/pipeline-agent.log"


    # ── Stale job / pipeline monitoring ───────────────────────────────────────
    stale_job_warn_min: int = 60          # alert when job runs longer than this
    stale_job_critical_min: int = 120     # second (critical) alert threshold
    stale_task_warn_min: int = 45         # alert when a single task hangs
    stale_pending_warn_min: int = 15      # alert when stuck in PENDING/BLOCKED
    stale_terminating_min: int = 10       # alert when stuck in TERMINATING
    stale_pipeline_min: int = 20          # alert when DLT stuck in INITIALIZING
    stale_realert_min: int = 30           # re-alert interval for persistent issues
    stale_auto_cancel: bool = False       # auto-cancel abandoned runs (careful!)

    # Derived flag
    email_enabled: bool = False

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            databricks_host=os.environ["DATABRICKS_HOST"].rstrip("/"),
            databricks_token=os.environ["DATABRICKS_TOKEN"],
            openai_api_key=os.environ["OPENAI_API_KEY"],
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            poll_interval_sec=int(os.getenv("POLL_INTERVAL_SEC", "60")),
            lookback_minutes=int(os.getenv("LOOKBACK_MINUTES", "60")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_backoff_base_min=float(os.getenv("RETRY_BACKOFF_BASE_MIN", "2.0")),
            smtp_host=os.getenv("SMTP_HOST", ""),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_tls=os.getenv("SMTP_TLS", "true").lower() == "true",
            smtp_user=os.getenv("SMTP_USER", ""),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            smtp_from=os.getenv("SMTP_FROM", ""),
            email_to=_csv("EMAIL_TO"),
            stale_job_warn_min=int(os.getenv("STALE_JOB_WARN_MIN", "60")),
            stale_job_critical_min=int(os.getenv("STALE_JOB_CRITICAL_MIN", "120")),
            stale_task_warn_min=int(os.getenv("STALE_TASK_WARN_MIN", "45")),
            stale_pending_warn_min=int(os.getenv("STALE_PENDING_WARN_MIN", "15")),
            stale_terminating_min=int(os.getenv("STALE_TERMINATING_MIN", "10")),
            stale_pipeline_min=int(os.getenv("STALE_PIPELINE_MIN", "20")),
            stale_realert_min=int(os.getenv("STALE_REALERT_MIN", "30")),
            stale_auto_cancel=os.getenv("STALE_AUTO_CANCEL", "false").lower() == "true",
            log_file=os.getenv("LOG_FILE", "/var/log/pipeline-agent.log"),
        )

    def validate(self) -> None:
        import logging
        log = logging.getLogger("config")

        missing = []
        if not self.databricks_host:  missing.append("DATABRICKS_HOST")
        if not self.databricks_token: missing.append("DATABRICKS_TOKEN")
        if not self.openai_api_key:   missing.append("OPENAI_API_KEY")
        if missing:
            raise ValueError("Missing required config:\n  " + "\n  ".join(missing))

        if self.smtp_host and self.email_to:
            self.email_enabled = True
            log.info("Email : %s → %s", self.smtp_host, ", ".join(self.email_to))
        else:
            log.warning("No email configured — failures logged to console only.")
