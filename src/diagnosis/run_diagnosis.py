import sys
sys.path.insert(0, "/opt/workspace/src/diagnosis")

from pyspark.sql import SparkSession
from snapshot_extractor import SnapshotExtractor
from anomaly_detector import AnomalyDetector
from llm_diagnostics import LLMDiagnostics
from slack_notifier import SlackNotifier


if __name__ == "__main__":
    ss = SparkSession.builder.appName("DiagnosisSnapshotCheckJob").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    extractor = SnapshotExtractor(ss)
    diffs = extractor.extract_all_tables()

    if not diffs:
        raise RuntimeError("[Diagnosis] No snapshot data could be extracted from any table. Check Iceberg catalog and S3 connectivity.")

    detector = AnomalyDetector()
    anomalies = detector.detect(diffs)

    print(f"\n[Diagnosis] Tables scanned : {len(diffs)}")
    print(f"[Diagnosis] Anomalies found: {len(anomalies)}")

    if detector.should_trigger_llm(anomalies):
        diagnostics = LLMDiagnostics()
        result = diagnostics.diagnose(anomalies)
        print(f"\n[Diagnosis] Severity : {result.overall_severity.upper()}")
        print(f"[Diagnosis] Summary  : {result.summary}")
        for action in result.recommended_actions:
            print(f"[Diagnosis]   -> {action}")
        SlackNotifier().notify(anomalies, result)
    else:
        print("[Diagnosis] All clear — no LLM call needed.")
