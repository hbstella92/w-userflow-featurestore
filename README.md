# **w-userflow-featurestore**

실시간 이벤트 형태의 유저 행동 데이터를 시뮬레이션하여, **스트리밍·배치 혼합 환경에서 데이터 정합성·재처리·운영 안정성을 고려한 Feature 생성 파이프라인의 설계 판단을 검증**한 개인 프로젝트
<br>
<br>
<br>
<br>

## 1. What problem this project solves (이 프로젝트가 해결하려는 문제)

유저 행동 이벤트 데이터는 분석 목적과 Feature 요구사항에 따라 **event, session, aggregate 등 서로 다른 의미 단위로 해석될 필요가 있습니다.**


하지만 하나의 파이프라인에서 이처럼 서로 다른 의미 단위를 함께 다루기 시작하면, 정합성과 재처리 기준을 일관되게 유지하기 어려워집니다.
<br>
<br>

예를 들어,
- Raw 데이터는 event 단위인데
- 사용자 행동 흐름 분석은 session 단위를 요구하며
- 비즈니스 지표나 Feature 생성은 aggregate 단위를 요구합니다.

<br>
이러한 서로 다른 의미 단위를 함께 처리할 경우, 다음과 같은 운영 문제가 반복적으로 발생합니다.


- 서로 다른 의미 단위가 함께 처리되면서 **정합성 판단 기준이 불명확해지는 문제**
- 과거 데이터 정정이나 로직 변경 시 **어디까지 다시 계산해야 하는지 판단하기 어려운 문제**
- 장애 발생 시 **복구 범위를 명확히 정하기 어려운 운영 문제**
<br>

본 프로젝트는 하나의 의미 단위를 기준으로 문제를 해결하려 하지 않습니다.  
대신 **event, session, aggregate를 각각 명확한 책임을 가진 레이어로 분리**하여 정합성, 재처리, 복구 판단 기준을 구조적으로 관리할 수 있는 데이터 파이프라인을 설계·구현했습니다.
<br>
<br>
<br>
<br>

## 2. Architecture & Data Flow (아키텍처 & 데이터 흐름)

본 프로젝트는 서로 다른 의미 단위 (event / session / aggregate)를 정합성·재처리·복구 기준까지 함께 관리하기 위해, 의미 단위별 책임을 **레이어** (Bronze / Silver / Gold)로 분리하는 메달리온 아키텍처를 채택했습니다.
<br>
<br>
<br>

### Design Principles (핵심 설계 방향)

- **정합성 범위를 Silver 레이어로 제한**  
  → 문제는 Silver에서 해결하고, Gold로 전파되지 않도록 함
- **스트리밍과 배치의 역할 분리**  
  → 스트리밍은 Raw 적재, 배치는 정제·집계에 집중
- **증분 처리만을 전제로 하지 않는 재처리 구조**  
  → 운영 상황에 따라 필요 시 전체 재처리로 전환되는 구조
- **검증된 데이터만 최종 Feature로 전달**  
  → Silver 단계에서 정합성이 확인된 데이터만 Gold 집계에 사용
- **복구 단위를 레이어별 의미 단위로 고정**  
  → Bronze(checkpoint), Silver(session), Gold(partition)
<br>
<br>

### Data Flow (데이터 흐름)

```
Simulated User Events
        ↓
      Kafka
        ↓
Spark Structured Streaming
        ↓
Iceberg (Bronze: Event / raw)
        ↓
Airflow + Spark Batch Job
        ↓
Iceberg (Silver: Session / cleansed)
        ↓
Airflow + Spark Batch Job
        ↓
Iceberg (Gold: Aggregate / features)
        ↓
      Trino
        ↓
     Grafana
```
<br>
<br>

### Responsibility by Layer (레이어별 역할)

- **Bronze (event)**
  - Raw 이벤트를 가능한 그대로 보존 (append-only)
  - 이후 정합성 로직 변경이나 재처리의 기준 원본 역할

- **Silver (session)**
  - 중복 제거, null 처리, 타입 캐스팅 등 정합성 로직을 적용하고
  - 이벤트를 session 단위로 묶어서 사용자 행동 흐름을 구성

- **Gold (aggregate)**
  - Silver를 기반으로 Feature 집계를 생성
  - 최종 결과 레이어로서 검증된 데이터만 담도록 운영
<br>
<br>
<br>

## 3. Design Decisions (설계 결정 사항)
<br>

### Decision 1. 정합성 및 상태 판별 책임을 Silver 레이어에 집중


유저 행동 데이터는 event 단위로 수집되며, 지연, 중복, 순서 뒤바뀜 등으로 인해 이벤트 자체만으로는 사용자의 실제 행동 흐름과 행동 상태를 정확히 판단하기 어렵습니다.


본 프로젝트에서는 이러한 특성을 고려하여, 정합성 보정과 함께 **사용자의 행동 흐름 구성과 행동 상태(완독, 이탈 등)의 판별 책임을 Silver 레이어에 집중**하는 선택을 했습니다.
<br>
<br>

- **Bronze 레이어**는 Raw 이벤트를 가능한 그대로 보존하는 역할에 집중합니다.  
  - 이벤트 단위에서는 사용자 행동의 전체 흐름을 알 수 없기 때문에, 이 시점에서의 정합성 판단이나 상태 판별은 불완전한 정보에 기반한 성급한 판단이 될 수 있습니다.

- **Silver 레이어**는 이벤트를 session 단위로 재구성하여 사용자 행동 흐름이 처음으로 의미 있게 완성되는 지점입니다.  
  - 그렇기 때문에 이 단계에서 콘텐츠 완독 여부, 중간 이탈, timeout 이탈 등 **행동 상태를 일관된 기준으로 처음 정의할 수 있습니다.**

- **Gold 레이어**는 Silver 결과를 기반으로 여러 Feature/집계를 생성하는 단계입니다.  
  - 이 단계에서 상태 판별을 수행할 경우, 동일한 판단 로직이 Feature별로 중복되기 쉽고, 로직 변경이나 오류 수정 시 재계산 범위를 관리하기 어려워집니다.
<br>

따라서 본 프로젝트에서는 상태 판별과 정합성 보정 로직을 Silver에 집중시키고, Gold는 판별된 session 결과들을 집계하는 역할로 단순화했습니다.


이 선택은 판단 기준의 소스를 한 곳으로 고정하여, 로직 변경 및 재처리 범위를 일관되게 관리하기 위한 설계 결정입니다.
<br>
<br>
<br>
<br>

### Decision 2. Silver 레이어에서 증분 처리와 전체 재처리의 판단 기준을 명확히 분리


유저 행동 데이터 파이프라인에서는 증분 처리를 통해 효율적으로 데이터를 처리하는 것도 중요하지만,  
과거 데이터 정정, backfill, 로직 변경이 발생하는 경우에는 증분 처리만으로는 결과의 정합성을 보장하기 어렵습니다.


반대로 항상 전체 재처리를 수행하면 정합성은 확보할 수 있지만, 운영 비용과 처리 시간이 과도하게 증가합니다.

본 프로젝트에서는 이러한 trade-off를 고려하여, **증분 처리와 전체 재처리 중 어떤 전략을 선택할지에 대한 판단을 Silver 레이어에서 명확히 수행하도록 설계**하였습니다.


이를 위해 **Apache Iceberg의 snapshot 메타데이터를 활용하여** 이전 처리 시점과 현재 테이블 상태 간의 관계를 확인하고, 관계가 유지되는 경우에는 증분 처리를, 관계가 깨진 경우에는 전체 재처리를 전환되도록 구성했습니다.


이러한 구조를 통해 재처리 전략을 운영자의 수동 판단이나 임시 조치에 의존하지 않고, 데이터 상태에 기반해 일관되게 선택할 수 있도록 했습니다.

이 선택은 항상 증분 처리의 효율을 극대화하기 위함이 아니라, 정합성이 깨지는 상황에서 명확한 기준으로 전체 재처리를 할 수 있도록 하기 위한 설계 결정입니다.
<br>
<br>
<br>
<br>

### Decision 3. Silver 적재 완료 검증을 기반으로 Gold 실행을 제어


Gold 레이어는 최종 Feature·집계 결과를 생성하는 단계로, 입력 데이터(Silver)가 불완전한 상태에서 실행되면, 잘못된 Feature가 생성되고 이후 수정·재처리 비용이 커질 수 있습니다.


본 프로젝트에서는 이를 방지하기 위해, **Airflow DAG에서 Gold 집계 실행 전에 Silver 적재 완료 여부를 사전 검증**하고, 조건을 만족하지 않으면 Gold 실행을 중단하도록 구성했습니다.


이를 위해 Iceberg Silver 테이블의 날짜 파티션 경로에 대해 S3 오브젝트(parquet 파일) 수를 확인하여 검증을 수행하고, 검증 태스크가 통과한 경우에만 Gold 집계 태스크들이 순차 실행되도록 구성했습니다.
<br>
<br>
<br>
<br>

### Decision 4. 레이어별 의미 단위에 맞춰 복구 기준을 분리


스트리밍·배치가 혼합된 파이프라인에서는 모든 장애를 동일한 기준으로 복구할 경우, 데이터의 의미 단위와 복구 단위가 어긋나면서 중복 반영이나 부분 상태에 의존한 계산이 발생하기 쉽습니다.
<br>

본 프로젝트에서는 이러한 문제를 방지하기 위해, 각 레이어가 다루는 **의미 단위에 맞춰 복구 기준을 분리**하는 선택을 했습니다.
<br>

- **Bronze 레이어**
  - event 단위 스트리밍 적재 단계로, 스트리밍 checkpoint를 기준으로 재시작하여 이벤트 손실 없이 처리를 이어갈 수 있도록 합니다.

- **Silver 레이어**
  - session key(session_id + 주요 차원키) 기준으로 `MERGE INTO` upsert를 수행해, 재처리 시 동일 session이 중복 누적되지 않고 최신 상태로 갱신되도록 했습니다.

- **Gold 레이어**
  - 집계 및 Feature 생성 단계로, 집계 파티션 단위(날짜)를 기준으로 복구하도록 구성했습니다. 이는 분석 및 Feature 소비 관점에서 가장 자연스러운 복구 단위라고 판단했습니다.
<br>

이와 같이 레이어별로 서로 다른 복구 기준을 적용함으로써, **장애 발생 시 복구 범위를 명확히 판단할 수 있도록** 했습니다. 또한 정합성 로직 변경이나 재처리 시에도 **불필요한 전체 재실행을 줄이고, 복구 판단을 단순화**했습니다.
<br>
<br>
<br>
<br>

## 4. Technical Choices (기술 선택 기준)

본 프로젝트에서의 기술 선택은 앞서 정의한 설계 판단을 구현하기에 적합한지를 기준으로 이루어졌습니다.
<br>
<br>

- **Kafka**
  - Producer/Consumer 분리를 통해 이벤트 스트림을 안정적으로 버퍼링 가능
  - 소비자(Spark) 장애 발생 시에도 retention 기간 내 재소비가 가능해 **스트리밍 복구 시나리오 검증에 적합**하다고 판단

- **Spark Structured Streaming**
  - Kafka 소스와의 통합이 안정적
  - 스트리밍과 배치를 동일한 엔진으로 처리할 수 있어, Bronze 스트리밍 적재와 Silver/Gold 배치 처리를 일관된 방식으로 구성 가능
  - Checkpoint 기반 재시작을 통해 **처리 상태를 명확히 관리** 가능

- **Apache Iceberg**
  - Snapshot 메타데이터를 통해 증분 처리와 전체 재처리를 **구조적 기준으로 분기**할 수 있어서 재처리 안정성 검증에 적합하다고 판단
  - 메타데이터 기반 원자적 커밋으로 배치 실패 시 **부분 반영 위험을 줄이고**, 일관된 snapshot을 유지 가능
  - Spark/Trino 등 여러 엔진에서 동일 테이블을 일관된 snapshot 기준으로 사용할 수 있는 오픈 테이블 포맷

- **Apache Airflow**
  - 레이어 간 실행 순서와 조건을 DAG로 명확히 표현 가능
  - 데이터 상태(Silver 적재 완료 여부 등)에 기반한 **조건부 실행 제어**를 구현하기에 적합

- **Trino**
  - Iceberg 테이블을 SQL로 직접 조회하여 결과를 **빠르게 검증/조회**하는 쿼리 엔진으로 활용

- **Grafana**
  - 파이프라인이 생성한 Feature를 **대시보드로 시각화하여 결과를 지속적으로 확인할 수 있는 인터페이스**로 활용
<br>
<br>
<br>
<br>

## 5. How to Run (프로젝트 실행 방법)

본 프로젝트는 `Kafka, Airflow, Spark, Iceberg, Trino, Grafana` 등 여러 컴포넌트로 구성되어 있으며, **Docker Compose 기반으로 실행 환경을 먼저 구성한 후** Kafka 이벤트 생성 및 파이프라인을 실행합니다.
<br>

아래는 **테스트 환경 기준의 전체 실행 흐름**입니다.
<br>
<br>
<br>

### 5.0. Docker Compose 기반 실행 환경 구성


Kafka, Airflow, Iceberg Catalog, Trino, Grafana 등 파이프라인 구성 요소를 **Docker Compose로 한 번에 기동**합니다.
<br>
<br>

#### 5.0.1. 컨테이너 초기화 및 빌드

```
# 프로젝트 최상단 디렉토리 진입
cd w-userflow-featurestore


# 기존 컨테이너 및 볼륨 정리
docker compose down -v


# 이미지 캐시 없이 재빌드
docker compose build --no-cache
```

> 이전 실행 환경이나 볼륨 상태로 인한 오류를 방지하기 위해  
> 초기 실행 시 `down -v` 및 `--no-cache` 빌드를 권장합니다.
<br>

#### 5.0.2. Airflow 초기화 컨테이너 실행

```
# Airflow metadata DB 초기화 및 기본 설정
docker compose up -d airflow-init
```

> Airflow는 최초 실행 시 metadata DB 초기화가 필요하며,  
> 해당 작업은 `airflow-init` 컨테이너에서 수행됩니다.
<br>

#### 5.0.3. 전체 서비스 기동

```
# 전체 컨테이너 실행
docker compose up -d
```
<br>

#### 5.0.4. 컨테이너 상태 확인 (선택)

```
docker compose ps
```

컨테이너가 정상적으로 기동되면 다음 단계로 **Kafka 이벤트 생성 및 DAG 실행**을 진행합니다.
<br>
<br>
<br>
<br>
<br>

### 5.1. Kafka 이벤트 생성 (Simulator)

유저 행동 이벤트는 **Kafka Producer 기반의 시뮬레이터**를 통해 생성됩니다.

```
# 프로젝트 최상단 디렉토리 진입
cd w-userflow-featurestore


# 가상환경 활성화
source .venv/bin/activate


# Kafka Producer 실행
cd src/kafka
python faker_producer.py --sessions {원하는 세션 수}
```

- **지정한 session 수만큼** 유저 행동 이벤트를 생성
- 생성된 이벤트는 Kafka topic으로 프로듀싱 됨
<br>
<br>
<br>

### 5.2. Airflow 설정 및 파이프라인 실행

Batch 파이프라인(Silver / Gold)은 Airflow DAG을 통해 오케스트레이션됩니다.
<br>
<br>

#### 5.2.1. Airflow Connection 설정
Airflow UI에서 아래 Connection들을 사전에 등록합니다.

- **spark_default**
  - Spark Submit을 통한 Batch Job 실행용
- **aws_default**
  - Iceberg 테이블이 저장된 S3 Object Storage 접근용
- **slack_webhook**
  - DAG 실패 알림용 (선택)

> Connection 상세 값은 환경(Spark 실행 모드, AWS 계정 credential, Slack webhook 설정)에 따라 달라질 수 있습니다.
<br>

#### 5.2.2. DAG 실행 순서

1. **Bronze DAG 실행**
    - Kafka 메세지를 소비하여 Bronze 레이어에 Raw 이벤트 적재
    - 필요 시 수동 trigger

2. **Silver DAG 활성화**
    - Bronze 데이터를 입력으로 정제 및 session 단위 재구성
    - 10분 단위 스케줄 실행

3. **Gold DAG 활성화**
    - Silver 데이터를 기반으로 Feature 및 집계 생성
    - 하루 1회 스케줄 실행
<br>

#### 실행 흐름 요약

```
Kafka Simulator 실행
        ↓
Bronze DAG (Streaming 적재)
        ↓
Silver DAG (정제 / Sessionization)
        ↓
Gold DAG (Feature 생성)
```

이 실행 흐름을 통해 `Kafka → Spark → Iceberg` 기반의 스트리밍·배치 혼합 파이프라인을 단계적으로 확인할 수 있습니다.
<br>
<br>
<br>
