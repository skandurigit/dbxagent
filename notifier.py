"""
notifier.py — SMTP email notifications. No webhooks, no approval buttons.

Notification types:
  1. send_failure_alert  — new failure with LLM analysis
  2. send_fix_applied    — fix was applied automatically
  3. send_escalation     — LLM says escalate (needs human attention)
"""
from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING, Dict, Optional

from config import Config

if TYPE_CHECKING:
    from analyzer import Analysis

logger = logging.getLogger(__name__)


# ── HTML email template ───────────────────────────────────────────────────────

_EMAIL = """\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8">
<style>
  body  {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #f5f7fa; margin: 0; padding: 20px; }}
  .wrap {{ max-width: 620px; margin: 0 auto; background: #fff;
           border-radius: 10px; overflow: hidden;
           box-shadow: 0 2px 12px rgba(0,0,0,.08); }}
  .hdr  {{ background: {hdr_color}; color: #fff; padding: 24px 28px; }}
  .hdr h1 {{ margin: 0; font-size: 1.2rem; }}
  .hdr p  {{ margin: 6px 0 0; opacity: .85; font-size: .875rem; }}
  .body {{ padding: 24px 28px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
  td    {{ padding: 9px 10px; border-bottom: 1px solid #e2e8f0;
           font-size: .875rem; vertical-align: top; }}
  td:first-child {{ font-weight: 600; width: 36%; color: #4a5568; }}
  .ftr  {{ background: #f7fafc; padding: 14px 28px;
           font-size: .75rem; color: #a0aec0; text-align: center; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="hdr"><h1>{title}</h1><p>{subtitle}</p></div>
  <div class="body"><table>{rows}</table></div>
  <div class="ftr">Databricks Reliability Agent &nbsp;·&nbsp; {timestamp}</div>
</div>
</body>
</html>
"""


def _row(label: str, value: str) -> str:
    return f"<tr><td>{label}</td><td>{value or '—'}</td></tr>"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "—"


# ── Notifier ──────────────────────────────────────────────────────────────────

class Notifier:

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    def send_failure_alert(self, failure: Dict, analysis: "Analysis") -> None:
        """Email on every new failure with full LLM analysis."""
        name   = failure.get("name", "unknown")
        ftype  = failure.get("type", "").upper()
        action = analysis.recommended_action.upper()

        rows = "".join([
            _row("Type",           ftype),
            _row("Name",           name),
            _row("Root Cause",     analysis.root_cause),
            _row("LLM Recommends", action),
            _row("Confidence",     analysis.confidence),
            _row("Reasoning",      analysis.reasoning),
            _row("Fix Suggestion", analysis.config_changes),
            _row("Run URL",        failure.get("run_url", "")),
        ])

        self._send(
            subject=f"🚨 {ftype} Failure: {name} — {action}",
            hdr_color="#dc2626",
            title=f"🚨 {ftype} Failure — {name}",
            subtitle=f"LLM recommends: {action}  |  Confidence: {analysis.confidence}",
            rows=rows,
        )

    def send_fix_applied(self, failure: Dict, analysis: "Analysis", action_taken: str) -> None:
        """Email confirmation after a fix is automatically applied."""
        name = failure.get("name", "unknown")
        rows = "".join([
            _row("Name",       name),
            _row("Action",     action_taken),
            _row("Root Cause", analysis.root_cause),
            _row("Applied at", _now()),
        ])
        self._send(
            subject=f"✅ Fix Applied: {name}",
            hdr_color="#16a34a",
            title=f"✅ Fix Applied — {name}",
            subtitle=f"Action taken: {action_taken}",
            rows=rows,
        )

    def send_escalation(self, failure: Dict, analysis: "Analysis") -> None:
        """Email when LLM says the issue needs human attention."""
        name = failure.get("name", "unknown")
        rows = "".join([
            _row("Name",           failure.get("name", "unknown")),
            _row("Type",           failure.get("type", "").upper()),
            _row("Root Cause",     analysis.root_cause),
            _row("Reasoning",      analysis.reasoning),
            _row("Fix Suggestion", analysis.config_changes),
            _row("Run URL",        failure.get("run_url", "")),
        ])
        self._send(
            subject=f"⚠️ Escalation Required: {name}",
            hdr_color="#7c3aed",
            title=f"⚠️ Escalation Required — {name}",
            subtitle="This failure needs manual investigation.",
            rows=rows,
        )

    # ── SMTP transport ────────────────────────────────────────────────────────

    def _send(
        self,
        subject: str,
        hdr_color: str,
        title: str,
        subtitle: str,
        rows: str,
    ) -> None:
        cfg = self._cfg
        if not cfg.smtp_host or not cfg.email_to:
            return

        html = _EMAIL.format(
            hdr_color=hdr_color,
            title=title,
            subtitle=subtitle,
            rows=rows,
            timestamp=_now(),
        )

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = cfg.smtp_from or cfg.smtp_user
            msg["To"]      = ", ".join(cfg.email_to)
            msg.attach(MIMEText(html, "html", "utf-8"))

            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=15) as server:
                if cfg.smtp_tls:
                    server.starttls()
                if cfg.smtp_user:
                    server.login(cfg.smtp_user, cfg.smtp_password)
                server.sendmail(msg["From"], cfg.email_to, msg.as_string())

            logger.info("Email sent: %s → %s", subject, cfg.email_to)

        except smtplib.SMTPAuthenticationError:
            logger.error("Email auth failed — check SMTP_USER / SMTP_PASSWORD")
        except smtplib.SMTPConnectError:
            logger.error("Cannot connect to %s:%d", cfg.smtp_host, cfg.smtp_port)
        except Exception as e:
            logger.error("Email error: %s", e)
