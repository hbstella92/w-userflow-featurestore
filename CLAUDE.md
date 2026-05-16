# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data engineering project that validates design decisions for a feature generation pipeline handling user behavior event data. It uses a **medallion architecture (Bronze → Silver → Gold)** over Apache Iceberg to manage data consistency, reprocessing, and operational stability across three semantic units: events, sessions, and aggregates.

## Technical Stack
- Kafka : 7.5.2-ccs
- Spark : v3.5.3
- Airflow : v2.10.2
- Iceberg : REST Catalog
- Python 3.10.12
- LLM : Claude API

## Architecture & Data Flow

```
Faker (simulated events)
    ↓
Kafka  (topic: webtoon_user_events_v2, 12 partitions)
    ↓
Bronze  – Spark Structured Streaming → Iceberg (append-only raw events)
    ↓
Silver  – Spark Batch → Iceberg (sessionization, dedup, state determination)
    ↓
Gold    – Spark Batch → Iceberg (5 aggregation tables)
    ↓
Trino (SQL) → Grafana (dashboards)
```

### Layer Responsibilities

- **Bronze**: Raw event ingestion, append-only. Recovery via Spark checkpoint / Kafka offsets.
- **Silver**: Dedup, null handling, type casting, and session state determination (`complete/exit/incomplete`). All consistency logic lives here. Uses Iceberg snapshot lineage to auto-select incremental vs. full reprocessing. Recovery via `MERGE INTO` upsert on session key.
- **Gold**: Aggregation only — runs after Silver partition validation. Five tables: `user_daily_metrics`, `webtoon_daily_metrics`, `webtoon_episode_daily_metrics`, `country_daily_metrics`, `platform_device_daily_metrics`. Recovery by date partition.

### Key Design Decisions

1. **Silver centralizes session state** — Bronze stays raw; Gold only aggregates already-validated sessions. This prevents duplication and simplifies reprocessing.
2. **Snapshot lineage gates Silver processing** — Silver DAG checks if Bronze's current snapshot is a descendant of the last-seen snapshot. Lineage intact → incremental; lineage broken → full reprocess.
3. **Silver file count gates Gold execution** — Gold DAG's `check_silver_file_count` task expects 140 parquet files per daily partition before Gold tasks run.

## Running the Stack

### Start / Stop

```bash
docker compose down -v          # Clean slate (drops volumes)
docker compose build --no-cache
docker compose up -d airflow-init
docker compose up -d
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8088 | byeolong2 / adminadmin12 |
| Spark Master | http://localhost:8080 | — |
| Kafka UI (Kafdrop) | http://localhost:9000 | — |
| Trino | http://localhost:8085 | — |
| Grafana | http://localhost:3000 | — |

### Running the Pipeline

```bash
# 1. Generate events (activate venv first)
cd src/kafka && python faker_producer.py --sessions 1000

# 2. Trigger Bronze DAG manually in Airflow UI (streams until stopped)
# 3. Silver DAG runs every 10 min automatically
# 4. Gold DAG runs daily automatically
```

### Airflow Connections (one-time setup via Admin → Connections)

- **spark_default**: Type=Spark, Host=spark://spark-master:7077, Port=7077
- **aws_default**: Type=Amazon Web Services, Extras with `aws_access_key_id` / `aws_secret_access_key`
- **slack_webhook** (optional): Type=HTTP, Password=Slack webhook URL

## Validating Data via Trino

```sql
SELECT COUNT(*), COUNT(DISTINCT event_id)
FROM iceberg.bronze.webtoon_user_events_raw
WHERE datetime = CAST(CURRENT_DATE AS DATE);

SELECT COUNT(DISTINCT session_id), SUM(is_complete), SUM(is_exit)
FROM iceberg.silver.webtoon_user_session_events
WHERE datetime = CAST(CURRENT_DATE AS DATE);
```

## Common Tasks → Key Files

| Task | File |
|------|------|
| Change event schema | [src/spark/bronze/bronze_load_raw_data.py](src/spark/bronze/bronze_load_raw_data.py) (`raw_event_schema`) |
| Sessionization logic | [src/spark/silver/silver_user_session_events.py](src/spark/silver/silver_user_session_events.py) |
| Add a Gold metric | Add file in [src/spark/gold/](src/spark/gold/), register in [airflow/dags/gold_daily_dag.py](airflow/dags/gold_daily_dag.py) |
| DAG schedule / Spark config | [airflow/dags/](airflow/dags/) |
| Kafka retention | `docker-compose.yml` (`KAFKA_LOG_RETENTION_*`) |

## Environment Variables (`.env`)

Key vars passed to all containers: `KAFKA_BOOTSTRAP_SERVERS`, `SPARK_MASTER`, `SPARK_PARQUET_WAREHOUSE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__FERNET_KEY`.

## Troubleshooting

- **Silver snapshot errors**: Reset the `bronze_last_snapshot` Airflow Variable to trigger a full reprocess.
- **Gold fails on file count check**: Silver partition is incomplete; wait for the Silver DAG to finish, then re-run Gold.
- **Spark OOM**: Increase executor memory in the `conf` dict of the relevant DAG's `SparkSubmitOperator`.

## New Feature: LLM-based Snapshot Diagnosis (구현 중)

### 목표
Iceberg snapshot 간 변화를 감지하고, Claude API를 통해 자연어 진단 결과를 생성하여
Slack으로 전송하는 자동화 파이프라인 추가.

### 구현 범위
- Iceberg snapshot 메타데이터 비교 (row count / schema / partition 변화 추출)
- 룰 기반 이상 감지 (LLM 호출 트리거 조건)
- Claude API 호출 및 진단 결과 파싱
- 기존 Slack webhook 연동 (slack_webhook Airflow Connection 재사용)
- Silver DAG 또는 Gold DAG 완료 후 자동 실행되는 Task로 연계

### 파일 위치 (예정)
- `src/diagnosis/snapshot_extractor.py` — Iceberg 메타데이터 추출
- `src/diagnosis/anomaly_detector.py` — 룰 기반 이상 감지
- `src/diagnosis/llm_diagnostics.py` — Claude API 호출 및 프롬프트 관리
- `src/diagnosis/slack_notifier.py` — Slack 메시지 포맷 및 전송
- `airflow/dags/diagnosis_dag.py` — 위 모듈들을 연계하는 DAG

### LLM 설정
- Model: claude-sonnet-4-20250514
- API Key: 환경변수 `ANTHROPIC_API_KEY` (.env에 추가 필요)
- 이상 감지된 경우에만 LLM 호출 (불필요한 API 비용 방지)

### 진단 대상 테이블 우선순위
1. `iceberg.silver.webtoon_user_session_events` (핵심 테이블)
2. `iceberg.bronze.webtoon_user_events_raw`
3. Gold 5개 테이블

## 구현 진행 상황
- [x] src/diagnosis/ 디렉토리 생성
- [x] snapshot_extractor.py 생성
- [x] anomaly_detector.py
- [ ] llm_diagnostics.py
- [ ] slack_notifier.py
- [ ] diagnosis_dag.py
