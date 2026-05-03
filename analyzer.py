"""analyzer.py — GPT-4o failure analysis with circuit breaker."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict

from openai import OpenAI
from config import Config

logger = logging.getLogger(__name__)

_SYSTEM = """
You are a senior Databricks Platform Reliability Engineer.
Analyse the failure and respond ONLY with a valid JSON object — no markdown.

Action decision rules:
- Transient errors (network timeout, cluster start-up, OOM, throttling) → retry
- Multi-task job with only some tasks failed, root cause is fixable → repair
- Schema mismatch, bad credentials, missing table, data quality → escalate
- Test / dev / intentionally cancelled run → ignore

JSON schema (all fields required):
{
  "root_cause": "<one concise sentence>",
  "recommended_action": "retry | repair | escalate | ignore",
  "reasoning": "<two or three sentences>",
  "config_changes": "<concrete suggestion, or empty string>",
  "confidence": "high | medium | low"
}
""".strip()


@dataclass
class Analysis:
    root_cause: str
    recommended_action: str   # retry | repair | escalate | ignore
    reasoning: str
    config_changes: str
    confidence: str
    raw: Dict = field(default_factory=dict)

    @classmethod
    def safe_escalate(cls, reason: str) -> "Analysis":
        return cls(
            root_cause=reason,
            recommended_action="escalate",
            reasoning="Analysis unavailable — defaulting to escalate for human review.",
            config_changes="",
            confidence="low",
        )


class Analyzer:
    _OPEN_THRESHOLD = 5
    _RESET_AFTER = 300  # seconds

    def __init__(self, cfg: Config) -> None:
        self._client = OpenAI(api_key=cfg.openai_api_key)
        self._model = cfg.openai_model
        self._failures = 0
        self._opened_at = 0.0

    def analyze(self, failure: Dict) -> Analysis:
        if self._circuit_open():
            return Analysis.safe_escalate("LLM circuit breaker open")

        prompt = self._build_prompt(failure)
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
                timeout=30,
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            self._failures = 0

            a = Analysis(
                root_cause=data.get("root_cause", "unknown"),
                recommended_action=self._valid(data.get("recommended_action", "escalate")),
                reasoning=data.get("reasoning", ""),
                config_changes=data.get("config_changes", ""),
                confidence=data.get("confidence", "medium"),
                raw=data,
            )
            logger.info("LLM → %s (%s) | %s", a.recommended_action, a.confidence, a.root_cause)
            return a

        except Exception as e:
            self._failures += 1
            if self._failures >= self._OPEN_THRESHOLD:
                self._opened_at = time.time()
            logger.error("OpenAI error: %s", e)
            return Analysis.safe_escalate(str(e))

    def _circuit_open(self) -> bool:
        if self._failures < self._OPEN_THRESHOLD:
            return False
        if time.time() - self._opened_at >= self._RESET_AFTER:
            self._failures = 0
            return False
        return True

    @staticmethod
    def _valid(action: str) -> str:
        return action if action in {"retry", "repair", "escalate", "ignore"} else "escalate"

    @staticmethod
    def _build_prompt(f: Dict) -> str:
        err = f"{f.get('error', '')}\n\n{f.get('error_trace', '')}".strip()
        if len(err) > 3500:
            err = err[:3500] + "\n...[truncated]"
        return (
            f"Type: {f.get('type', 'unknown').upper()}\n"
            f"Name: {f.get('name', 'unknown')}\n"
            f"ID: {f.get('pipeline_id') or f.get('job_id', '')}\n"
            f"Run/Update: {f.get('update_id') or f.get('run_id', '')}\n"
            f"URL: {f.get('run_url', '')}\n\n"
            f"Error:\n{err}"
        )
