from dataclasses import dataclass
from typing import Dict, List

from snapshot_extractor import SnapshotDiff


BRONZE_TABLE = "iceberg.bronze.webtoon_user_events_raw"
SILVER_TABLE = "iceberg.silver.webtoon_user_session_events"
INGESTION_TABLES = {BRONZE_TABLE, SILVER_TABLE}


@dataclass
class AnomalyResult:
    table_name: str
    severity: str  # "critical" | "warning"
    rule_name: str
    message: str
    snapshot_diff: SnapshotDiff


class AnomalyDetector:
    DELETION_RATIO_THRESHOLD = 0.3
    SPIKE_RATIO_THRESHOLD = 2.0
    SILVER_MIN_FILES = 140

    def detect(self, diffs: Dict[str, SnapshotDiff]) -> List[AnomalyResult]:
        anomalies: List[AnomalyResult] = []
        for diff in diffs.values():
            anomalies.extend(self._check_lineage_broken(diff))
            anomalies.extend(self._check_table_empty(diff))
            anomalies.extend(self._check_no_new_records(diff))
            anomalies.extend(self._check_excessive_deletions(diff))
            anomalies.extend(self._check_data_spike(diff))
            anomalies.extend(self._check_silver_low_file_count(diff))

        for a in anomalies:
            print(f"[AnomalyDetector] {a.severity.upper()} | {a.table_name} | {a.rule_name}: {a.message}")

        return anomalies

    def should_trigger_llm(self, anomalies: List[AnomalyResult]) -> bool:
        return len(anomalies) > 0

    def _check_lineage_broken(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        if not diff.lineage_intact:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="critical",
                rule_name="lineage_broken",
                message=(
                    f"Snapshot lineage is broken. "
                    f"current_snapshot_id={diff.current_snapshot_id}, "
                    f"previous_snapshot_id={diff.previous_snapshot_id}. "
                    "Full reprocessing may be required."
                ),
                snapshot_diff=diff,
            )]
        return []

    def _check_table_empty(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        if diff.total_records == 0:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="critical",
                rule_name="table_empty",
                message=f"Table has 0 total records at snapshot {diff.current_snapshot_id}.",
                snapshot_diff=diff,
            )]
        return []

    def _check_no_new_records(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        # Gold tables use overwrite-based aggregation; added_records=0 is not meaningful there
        if diff.table_name not in INGESTION_TABLES:
            return []
        if diff.previous_snapshot_id is not None and diff.added_records == 0:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="warning",
                rule_name="no_new_records",
                message=(
                    f"No records were added in the latest snapshot "
                    f"(snapshot_id={diff.current_snapshot_id}). "
                    "Possible ingestion stall."
                ),
                snapshot_diff=diff,
            )]
        return []

    def _check_excessive_deletions(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        if diff.deleted_records == 0:
            return []
        previous_total = diff.total_records + diff.deleted_records - diff.added_records
        ratio = diff.deleted_records / previous_total
        if ratio > self.DELETION_RATIO_THRESHOLD:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="warning",
                rule_name="excessive_deletions",
                message=(
                    f"Deletion ratio is {ratio:.1%} "
                    f"({diff.deleted_records} deleted out of ~{previous_total} previous records), "
                    f"exceeding threshold of {self.DELETION_RATIO_THRESHOLD:.0%}."
                ),
                snapshot_diff=diff,
            )]
        return []

    def _check_data_spike(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        if diff.total_records == 0 or diff.added_records == 0:
            return []
        ratio = diff.added_records / diff.total_records
        if ratio > self.SPIKE_RATIO_THRESHOLD:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="warning",
                rule_name="data_spike",
                message=(
                    f"Added records ({diff.added_records}) exceed "
                    f"{self.SPIKE_RATIO_THRESHOLD:.0f}x total records ({diff.total_records}). "
                    "Possible data duplication."
                ),
                snapshot_diff=diff,
            )]
        return []

    def _check_silver_low_file_count(self, diff: SnapshotDiff) -> List[AnomalyResult]:
        if diff.table_name != SILVER_TABLE:
            return []
        if diff.total_data_files < self.SILVER_MIN_FILES:
            return [AnomalyResult(
                table_name=diff.table_name,
                severity="warning",
                rule_name="silver_low_file_count",
                message=(
                    f"Silver partition has {diff.total_data_files} data files, "
                    f"below the expected minimum of {self.SILVER_MIN_FILES}. "
                    "Gold DAG gate may block execution."
                ),
                snapshot_diff=diff,
            )]
        return []
