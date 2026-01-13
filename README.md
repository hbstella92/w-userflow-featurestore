# **w-userflow-featurestore**

This project is an MVP focused on designing a simulated event-driven user behavior data pipeline using Kafka, Spark Structured Streaming, Apache Iceberg, AWS S3, Airflow, Trino, Grafana, Docker.

The goal is to explore data modeling, failure-aware design, and feature construction in streaming + batch hybrid environments, rather than production-level optimization or hard guarantees.
<br>
<br>
<br>

## Motivation
User behavior data arrives as streaming events that can be unordered and duplicated in real-world systems.

This project explores, from a design perspective, where non-determinism can be introduced in streaming pipelines and how it can be constrained through explicit processing boundaries and data modeling decisions.

The goal of this MVP is not to guarantee strict determinism, but to reason about trade-offs in reprocessing and feature generation in a streaming + batch architecture.

> Note: Input events are simulated using a Faker-based Kafka producer
<br>
<br>

## Architecture Overview
```
Kafka (simulated events)
    -> Spark Structured Streaming
        -> Iceberg tables on S3 (bronze/silver)
            -> Batch Aggregation
                -> Iceberg feature tables on S3 (gold)
                    -> Trino -> Grafana
```
* Streaming ingestion handles simulated user events, while batch jobs generate session-level and daily feature tables.
<br>
<br>

## Data Model
### Event Types
* enter
* scroll
* complete
* exit
Each event includes:
* user_id, session_id
* content identifiers (webtoon_id, episode_id)
* timestamp (UTC + local timezone)
* behavioral metrics (scroll ratio, dwell time)
* user environment (country, platform, device, browser, network_type)
<br>
<br>

## Key Design Decisions
#### 1. Snapshot-based consistency reasoning with Iceberg
Iceberg is used as the table format to reason about data consistency under failure and retry scenarios, allowing recovery decisions to be made without blindly trusting incremental results.

#### 2. Session-level behavior feature modeling
Session-level aggregates are explicitly constructed during batch processing, as session boundaries provide a more meaningful basis for behavior analysis than individual events.

#### 3. Separation of streaming and batch workloads
Streaming job is kept intentionally simple, focusing on event ingestion without feature logic, while batch jobs handle feature aggregation. This separation improves pipeline stability and simplifies reprocessing considerations.

#### 4. Orchestrated execution and observability
Airflow is used to control batch execution flow, with Slack notifications integrated for basic observability and failure awareness.
<br>
<br>
<br>

## Idempotency & Reprocessing
* This MVP does not attempt to fully guarantee idempotency, but explicitly documents how duplicate events would be handled in a production setting.

**Production-ready alternatives considered:**
* Partition overwrite during batch recomputation
* Iceberg MERGE/UPSERT
<br>
<br>

## How to Run
### Start infrastructure
docker compose build
docker compose up -d airflow-init
docker compose up -d

### Run Kafka producer
source .venv/bin/activate
python src/kafka/faker_producer.py --sessions 1000

### Run streaming ingestion and batch feature aggregation

Streaming ingestion and batch feature aggregation are orchestrated via Airflow.

For this MVP, DAGs are triggered manually in the following order to make execution boundaries explicit:

1. Enable and trigger the bronze ingestion DAG to start streaming ingestion.
2. Enable the silver and gold DAGs for downstream aggregation.
    (These DAGs are configured to run once upstream data becomes available.)

This manual execution flow is intentional, as the focus of the project is on validating pipeline design and data flow separation rather than full automation.
<br>
<br>
<br>

## What This Project is NOT
* Not a production-ready system
* Not a fully documented open-source project
This repository intentionally focuses on **design validation and decision-making**.
<br>
<br>

## Takeaways
This project highlighted that:
* Streaming pipelines should be designed assuming failures and retries will happen.
* Batch aggregation provides a safer point for recomputation than streaming job.
* Session-level aggregation provides a more meaningful unit for user behavior analysis than individual events.
<br>
<br>

## Tech Stack
* Kafka
* Spark Structured Streaming
* Apache Iceberg
* AWS S3
* Airflow
* Trino
* Grafana
* Docker
