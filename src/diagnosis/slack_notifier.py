import os
import requests
from typing import List, Optional

from anomaly_detector import AnomalyResult
from llm_diagnostics import DiagnosisResult


class SlackNotifier:
    def __init__(self):
        self.webhook_url: Optional[str] = os.environ.get("SLACK_WEBHOOK_URL") or None

    def notify(self, anomalies: List[AnomalyResult], result: DiagnosisResult) -> None:
        if not self.webhook_url:
            print("[SlackNotifier] SLACK_WEBHOOK_URL not set — skipping notification")
            return
        message = self._format_message(anomalies, result)
        try:
            resp = requests.post(
                self.webhook_url,
                json={"text": message},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            print("[SlackNotifier] Sent successfully")
        except Exception as e:
            print(f"[SlackNotifier] Failed to send: {e}")

    def _format_message(self, anomalies: List[AnomalyResult], result: DiagnosisResult) -> str:
        severity_emoji = "🚨" if result.overall_severity == "critical" else "⚠️"
        lines = [
            f"{severity_emoji} *Pipeline Anomaly Detected ({len(anomalies)} anomalies)*",
            "━━━━━━━━━━━━━━━━━━━━━━━",
            f"*Severity:* `{result.overall_severity.upper()}`",
        ]

        lines.append("\n*Detected Anomalies:*")
        for a in anomalies:
            rule_emoji = "🔴" if a.severity == "critical" else "🟡"
            lines.append(f"  {rule_emoji} `{a.rule_name}` | {a.table_name}")
            lines.append(f"      {a.message}")

        lines.append(f"\n*LLM Diagnosis:*\n{result.summary}")

        if result.table_analyses:
            lines.append("\n*Table Analysis:*")
            for ta in result.table_analyses:
                lines.append(f"  • {ta}")

        if result.recommended_actions:
            lines.append("\n*Recommended Actions:*")
            for action in result.recommended_actions:
                lines.append(f"  - {action}")

        return "\n".join(lines)
