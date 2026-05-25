import os
from dataclasses import dataclass
from typing import Optional, Dict
from datetime import datetime


def to_int_or_none(x):
    try:
        return int(x) if x not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def is_ancestor_snapshot(ss, table_name: str, start_id: int, end_id: int) -> bool:
    """Walk the snapshot parent chain from end_id upward; return True if start_id is an ancestor."""
    current_id = end_id

    while True:
        df = ss.sql(f"""
            SELECT parent_id
            FROM {table_name}.snapshots
            WHERE snapshot_id = {current_id}
        """)
        rows = df.collect()

        if not rows or rows[0]["parent_id"] is None:
            break

        parent_id = rows[0]["parent_id"]
        if parent_id is None:
            return False

        if parent_id == start_id:
            return True

        current_id = parent_id

    return False


@dataclass
class SnapshotDiff:
    table_name: str
    current_snapshot_id: int
    previous_snapshot_id: Optional[int]
    committed_at: datetime
    total_records: int
    added_records: int
    deleted_records: int
    total_data_files: int
    lineage_intact: bool


class SnapshotExtractor:
    DIAGNOSIS_TABLES = [
        "iceberg.bronze.webtoon_user_events_raw",
        "iceberg.silver.webtoon_user_session_events",
        "iceberg.gold.user_daily_metrics",
        "iceberg.gold.webtoon_daily_metrics",
        "iceberg.gold.webtoon_episode_daily_metrics",
        "iceberg.gold.country_daily_metrics",
        "iceberg.gold.platform_device_daily_metrics",
    ]

    def __init__(self, ss):
        self.ss = ss

    def get_latest_snapshots(self, table_name: str, n: int = 2):
        df = self.ss.sql(f"""
            SELECT snapshot_id, parent_id, committed_at, summary
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT {n}
        """)
        return df.collect()

    def _parse_summary_int(self, summary, key: str) -> int:
        if summary is None:
            return 0
        return to_int_or_none(summary.get(key, 0)) or 0

    def extract_snapshot_diff(self, table_name: str) -> Optional[SnapshotDiff]:
        rows = self.get_latest_snapshots(table_name, n=2)
        if not rows:
            print(f"[SnapshotExtractor] No snapshots found for {table_name}")
            return None

        current = rows[0]
        previous = rows[1] if len(rows) > 1 else None

        current_id = current["snapshot_id"]
        prev_id = previous["snapshot_id"] if previous else None
        summary = current["summary"]

        lineage_ok = True
        if prev_id is not None:
            lineage_ok = is_ancestor_snapshot(self.ss, table_name, prev_id, current_id)

        return SnapshotDiff(
            table_name=table_name,
            current_snapshot_id=current_id,
            previous_snapshot_id=prev_id,
            committed_at=current["committed_at"],
            total_records=self._parse_summary_int(summary, "total-records"),
            added_records=self._parse_summary_int(summary, "added-records"),
            deleted_records=self._parse_summary_int(summary, "deleted-records"),
            total_data_files=self._parse_summary_int(summary, "total-data-files"),
            lineage_intact=lineage_ok,
        )

    def extract_all_tables(self) -> Dict[str, SnapshotDiff]:
        results = {}
        for table in self.DIAGNOSIS_TABLES:
            try:
                diff = self.extract_snapshot_diff(table)
                if diff:
                    results[table] = diff
                    print(
                        f"[SnapshotExtractor] {table}: "
                        f"total={diff.total_records}, added={diff.added_records}, "
                        f"deleted={diff.deleted_records}, files={diff.total_data_files}, "
                        f"lineage_intact={diff.lineage_intact}"
                    )
            except Exception as e:
                print(f"[SnapshotExtractor] Failed to extract {table}: {e}")
        return results
