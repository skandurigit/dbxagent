"""main.py — Entry point for the Databricks 24/7 Reliability Agent."""
from __future__ import annotations

import logging
import signal
import sys
import time

from config import Config
from databricks_client import DatabricksClient
from analyzer import Analyzer
from notifier import Notifier
from monitor import Monitor


def _setup_logging(log_file: str) -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: list = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(log_file))
    except OSError as e:
        print(f"WARNING: cannot open log file {log_file}: {e}")
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    for noisy in ("urllib3", "openai", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    try:
        cfg = Config.from_env()
        cfg.validate()
    except (KeyError, ValueError) as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)

    _setup_logging(cfg.log_file)
    log = logging.getLogger("agent")

    log.info("=" * 56)
    log.info("  Databricks 24/7 Reliability Agent")
    log.info("=" * 56)
    log.info("  Host     : %s", cfg.databricks_host)
    log.info("  Poll     : every %ds", cfg.poll_interval_sec)
    log.info("  Lookback : %d min on first scan", cfg.lookback_minutes)
    log.info("  Email    : %s", "✓ " + cfg.smtp_host if cfg.email_enabled else "✗ (not configured)")
    log.info("=" * 56)

    db       = DatabricksClient(cfg.databricks_host, cfg.databricks_token)
    analyzer = Analyzer(cfg)
    notifier = Notifier(cfg)
    monitor  = Monitor(cfg, db, analyzer, notifier)

    stop = {"now": False}

    def _sig(sig, _frame):
        log.info("Shutdown signal received — stopping after current cycle…")
        stop["now"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT,  _sig)

    log.info("Monitoring ALL jobs and pipelines. Press Ctrl+C to stop.\n")

    while not stop["now"]:
        t0 = time.monotonic()
        try:
            monitor.run_cycle()
        except Exception as e:
            log.error("Unhandled error in cycle: %s", e, exc_info=True)
        remaining = cfg.poll_interval_sec - (time.monotonic() - t0)
        if remaining > 0 and not stop["now"]:
            time.sleep(remaining)

    log.info("Agent stopped.")


if __name__ == "__main__":
    main()
