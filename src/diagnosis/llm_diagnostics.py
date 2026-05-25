import os
import re
from dataclasses import dataclass, field
from typing import List

import anthropic

from anomaly_detector import AnomalyResult


SYSTEM_PROMPT = """You are a data pipeline reliability expert analyzing an Apache Iceberg-based data lakehouse.

## Pipeline Architecture (Medallion Pattern)

Bronze → Silver → Gold

- **Bronze** (`iceberg.bronze.webtoon_user_events_raw`): Raw event ingestion via Spark Structured Streaming from Kafka. Append-only. Recovery via Spark checkpoints and Kafka offsets.
- **Silver** (`iceberg.silver.webtoon_user_session_events`): Sessionization, dedup, null handling, type casting, and session state determination (complete/exit/incomplete). Uses Iceberg snapshot lineage to decide incremental vs. full reprocessing. Recovery via MERGE INTO upsert on session key.
- **Gold** (5 tables): Aggregation only — runs after Silver partition validation. Tables: `user_daily_metrics`, `webtoon_daily_metrics`, `webtoon_episode_daily_metrics`, `country_daily_metrics`, `platform_device_daily_metrics`. Recovery by date partition.

## Anomaly Rules and Their Significance

| Rule | Severity | Meaning |
|------|----------|---------|
| `lineage_broken` | CRITICAL | Snapshot parent chain broken — Silver cannot determine incremental vs. full reprocessing safely. Immediate action required. |
| `table_empty` | CRITICAL | Table has 0 records — complete data loss or pipeline never ran. |
| `no_new_records` | WARNING | Bronze or Silver received no new records — possible Kafka ingestion stall or upstream failure. Gold tables are excluded from this check since they use overwrite-based aggregation. |
| `excessive_deletions` | WARNING | Deletion ratio > 30% of previous total — may indicate unintended reprocessing or data quality issues. |
| `data_spike` | WARNING | Added records > 2x total records — possible duplication from replay or backfill. |
| `silver_low_file_count` | WARNING | Silver partition has < 140 parquet files — Gold DAG gate will block Gold execution. |

## Operational Context

- Silver DAG runs every 10 minutes. Gold DAG runs daily.
- Silver snapshot lineage is tracked via Airflow Variable `bronze_last_snapshot`. Resetting it triggers a full reprocess.
- Gold DAG requires exactly 140 parquet files in the Silver partition before executing.
- The pipeline serves a webtoon user behavior analytics system: Bronze ingests raw click/view events, Silver computes user sessions, Gold provides daily metrics for dashboards.

Your task: analyze the detected anomalies and provide a clear, actionable diagnosis for the on-call engineer."""


@dataclass
class DiagnosisResult:
    overall_severity: str
    summary: str
    table_analyses: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    raw_response: str = ""


class LLMDiagnostics:
    MODEL = "claude-haiku-4-5-20251001"

    def __init__(self):
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is not set")
        self.client = anthropic.Anthropic(api_key=api_key)

    def diagnose(self, anomalies: List[AnomalyResult]) -> DiagnosisResult:
        user_prompt = self._build_user_prompt(anomalies)
        response = self.client.messages.create(
            model=self.MODEL,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw_text = response.content[0].text
        print(f"[LLMDiagnostics] Claude responded ({response.usage.output_tokens} tokens)")
        return self._parse_response(raw_text, anomalies)

    def _build_user_prompt(self, anomalies: List[AnomalyResult]) -> str:
        lines = [f"=== Detected Anomalies ({len(anomalies)}) ===\n"]
        for i, a in enumerate(anomalies, 1):
            d = a.snapshot_diff
            lines.append(
                f"[{i}] Table: {a.table_name}\n"
                f"    Severity: {a.severity.upper()}\n"
                f"    Rule: {a.rule_name}\n"
                f"    Message: {a.message}\n"
                f"    Snapshot info:\n"
                f"      - Total records   : {d.total_records:,}\n"
                f"      - Added records   : {d.added_records:,}\n"
                f"      - Deleted records : {d.deleted_records:,}\n"
                f"      - Data files      : {d.total_data_files}\n"
                f"      - Lineage intact  : {d.lineage_intact}\n"
                f"      - Committed at    : {d.committed_at}\n"
            )
        lines.append(
            "Respond in exactly this format:\n\n"
            "[SUMMARY]\n"
            "(1-2 sentence overall assessment)\n\n"
            "[TABLE_ANALYSIS]\n"
            "(one line per table: 'table_name: analysis')\n\n"
            "[ACTIONS]\n"
            "- (action item 1)\n"
            "- (action item 2)\n"
            "..."
        )
        return "\n".join(lines)

    def _parse_response(self, raw_text: str, anomalies: List[AnomalyResult]) -> DiagnosisResult:
        overall_severity = "critical" if any(a.severity == "critical" for a in anomalies) else "warning"
        try:
            summary = self._extract_section(raw_text, "SUMMARY").strip()
            table_block = self._extract_section(raw_text, "TABLE_ANALYSIS")
            actions_block = self._extract_section(raw_text, "ACTIONS")

            table_analyses = [line.strip() for line in table_block.strip().splitlines() if line.strip()]
            recommended_actions = [
                line.lstrip("- ").strip()
                for line in actions_block.strip().splitlines()
                if line.strip().startswith("-")
            ]
            return DiagnosisResult(
                overall_severity=overall_severity,
                summary=summary,
                table_analyses=table_analyses,
                recommended_actions=recommended_actions,
                raw_response=raw_text,
            )
        except ValueError:
            print("[LLMDiagnostics] Warning: structured sections not found; using raw response as summary")
            return DiagnosisResult(
                overall_severity=overall_severity,
                summary=raw_text.strip(),
                raw_response=raw_text,
            )

    @staticmethod
    def _extract_section(text: str, section: str) -> str:
        pattern = rf"\[{section}\](.*?)(?=\[(?:SUMMARY|TABLE_ANALYSIS|ACTIONS)\]|$)"
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            raise ValueError(f"Section [{section}] not found in response")
        return match.group(1)
