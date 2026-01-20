# **w-userflow-featurestore**

실시간 이벤트 형태의 유저 행동 데이터를 시뮬레이션하여, 스트리밍·배치 혼합 환경에서 데이터 정합성·재처리·운영 안정성을 고려한 Feature 생성 파이프라인의 설계 판단을 검증한 개인 프로젝트
<br>
<br>
<br>

## 1. 프로젝트 목표 (Goals)

본 프로젝트의 목표는 **유저 행동 이벤트를 시뮬레이션하여 생성한 이벤트 데이터를 대상**으로

**스트리밍·배치 혼합 환경에서 신뢰 가능한 Feature를 안정적으로 생성하기 위한 파이프라인 구조를 설계·구현하고, 그 설계 판단의 타당성을 검증하는 것**입니다.
<br>
<br>

이를 위해 **다음과 같은 관점을 중심으로 파이프라인을 구성**했습니다.

- 스트리밍과 배치가 혼합된 환경에서의 데이터 정합성 보장
- 증분 처리와 전체 재처리 간의 trade-off 인식
- 장애 발생 시 복구 단위에 대한 운영 관점 고려
<br>

이러한 관점을 구현하기 위해 **Raw 데이터 보존, 레이어별 책임 분리(Bronze/Silver/Gold), Iceberg snapshot 기반 처리 전략을 중심으로 파이프라인을 설계·구현**했습니다.
<br>
<br>
<br>
<br>

## 2. 프로젝트 비목표 (Non-goals)

본 프로젝트는 **다음을 목표로 하지 않습니다.**
- 대규모 처리량 벤치마킹 또는 성능 경쟁
- 수 ms 단위의 실시간 Feature 서빙
- 멀티 클러스터 또는 클라우드 환경에서의 대규모 분산 운영 검증
<br>

본 프로젝트는 **소규모 환경을 전제로**
**파이프라인 구조 설계, 운영 안정성, 재처리 전략의 타당성 검증에 집중**했습니다.
<br>
<br>
<br>
<br>

## 3. 해결하고자 하는 문제 (Problem Statement)

서비스 이벤트 형태의 유저 행동 데이터는 단순히 이벤트를 빠르게 적재하는 것만으로는 **분석과 Feature 생성에 바로 활용하기 어렵다**는 한계가 있습니다.
<br>
<br>

특히 스트리밍 기반 이벤트 처리 환경에서는 **다음과 같은 문제가 반복적으로 발생**합니다.
- 이벤트 중복, 지연, 순서 뒤바뀜 등으로 인한 **데이터 정합성 문제**
- 증분 처리 효율을 우선할 경우, 과거 데이터 정정이나 backfill 상황에서 **전체 재처리 안정성 저하**
- 파이프라인 장애 발생 시, **어디까지 복구해야 하는지 명확하지 않은 복구 단위 문제**
- Raw 데이터, 정제 데이터, 집계 Feature가 혼재되면서 **책임 범위가 불분명해지는 데이터 구조**

이러한 문제는 단순한 `Kafka → Spark → DB` 형태의 파이프라인만으로는 구조적으로 해결하기 어렵습니다.
<br>
<br>

본 프로젝트에서는 유저 행동 이벤트를 **서비스 이벤트와 유사한 형태로 시뮬레이션**하고, 실제 운영 환경에서 발생할 수 있는 정합성·재처리·장애 복구 이슈를 구조적으로 다룰 수 있는 파이프라인 설계를 목표로 했습니다.

이를 위해 Raw 데이터 보존을 전제로 한 레이어 분리, 증분 처리와 전체 재처리를 모두 고려한 저장 구조, 데이터 상태에 기반한 파이프라인 실행 제어가 필요하다고 판단했습니다.
<br>
<br>
<br>
<br>

## 4. 아키텍처 (Architecture)

본 프로젝트의 아키텍처는 앞서 정의한 문제들(정합성, 재처리 안정성, 복구 단위, 책임 분리)을 구조적으로 해결하는 것을 목표로 설계되었습니다.
<br>
<br>

**핵심 설계 방향**은 다음과 같습니다.

- Bronze/Silver/Gold 레이어로 책임을 분리해 **정합성 이슈의 범위를 제한**
- 스트리밍은 Raw 적재, 배치는 정제·집계로 역할을 분리해 **운영 복잡도를 낮춤**
- 증분 처리와 전체 재처리를 **모두 허용하는 테이블/레이어 구조**를 설계
- 장애 발생 시 **레이어별 의미 단위(Streaming checkpoint / session / 집계 파티션)를 기준으로 복구 가능하도록 고려**

이를 위해 `Kafka → Spark → Iceberg(Bronze/Silver/Gold) → Trino/Grafana` 구조를 채택했습니다.
<br>
<br>
<br>

### 주요 구성 요소와 역할

**Kafka**
- 시뮬레이터(Producer)가 생성한 유저 행동 이벤트를 저장하는 중간 버퍼 역할
- 소비자(Spark) 장애, 네트워크 지연 상황에서도 **retention 기간 내 이벤트 재소비 가능**

**Spark Structured Streaming**
- Kafka로부터 이벤트를 스트리밍 방식으로 처리
- 최소한의 가공만 수행하여 **Bronze 레이어에 Raw 데이터 적재**
- Checkpoint 기반으로 장애 시 재시작 가능하도록 구성

**Apache Iceberg**
- Bronze/Silver/Gold 레이어의 테이블 포맷
- Snapshot 메타데이터를 기준으로 **증분 처리와 전체 재처리를 분기**할 수 있는 테이블 포맷

**Apache Airflow**
- Silver/Gold 배치 Spark Job 오케스트레이션
- 데이터 상태(Silver 적재 완료 여부, snapshot 관계 등)에 기반한 **조건부 실행 제어**

**Trino & Grafana**
- Gold 레이어에 적재된 Feature 조회 및 시각화
- 파이프라인 결과를 **검증 가능한 형태로 노출**
<br>
<br>
<br>

## 5. Data Flow

아래는 전체 데이터 흐름을 단순화한 구조입니다.

```
Simulated User Events
        ↓
      Kafka
        ↓
Spark Structured Streaming
        ↓
Iceberg (Bronze: Raw Events)
        ↓
Airflow-triggered Batch Jobs (Silver / Gold)
        ↓
Iceberg (Silver: Cleansed & Sessionized)
        ↓
Iceberg (Gold: Feature Tables)
        ↓
      Trino
        ↓
     Grafana
```
<br>

### Data Flow 설계 의도
- **Bronze**
  - Raw 이벤트를 가능한 한 그대로 보존
  - 재처리 및 정합성 검증의 기준점 역할
- **Silver**
  - 중복 제거, null 처리, 타입 캐스팅, session 단위 재구성
  - 정합성 책임을 집중시키는 레이어
- **Gold**
  - 분석 및 시각화를 위한 Feature 및 집계 결과
  - Silver 데이터가 정상적일 때만 생성되도록 제어

이러한 흐름을 통해 문제 발생 시 **어느 레이어에서 문제가 발생했는지 명확히 추적**할 수 있도록 설계했습니다.
<br>
<br>
<br>
<br>

## 6. 핵심 설계 결정 (Design Decisions)

본 프로젝트에서는 앞서 정의한 문제들(정합성, 재처리 안정성, 복구 단위, 책임 분리)을 **파이프라인 구조 설계 관점에서 해결하고자 했습니다.**

아래는 주요 설계 결정과 그 배경입니다.
<br>
<br>
<br>

### 6.1. Raw 데이터 보존을 전제로 한 Bronze 레이어 설계

**결정**
- Kafka에서 수집한 이벤트를 최소한의 가공만 수행한 상태로 Bronze 레이어에 적재
- Raw 데이터는 **append-only**로 관리

**배경**
- 스트리밍 환경에서는 이벤트 중복, 지연, 순서 뒤바뀜이 발생할 수 있음
- 초기에 과도한 정제나 집계를 수행할 경우, 이후 정합성 이슈 발생 시 **재처리 범위가 불명확해짐**

**기대 효과**
- 정합성 이슈 발생 시 **Raw 데이터를 기준으로 재처리 가능**
- 후속 레이어(Silver/Gold)의 책임 범위를 명확히 분리
<br>
<br>

### 6.2. Silver 레이어에서 정합성 책임 집중 (Session 단위 정제)

**결정**
- 정합성 관련 처리를 Silver 레이어에 집중
  - 중복 이벤트 제거
  - null 처리 및 타입 캐스팅
  - 이벤트를 session 단위로 재구성
- Gold 레이어 이전에 **의미 단위(session)** 를 명확히 정의

**배경**
- 이벤트 단위 재처리는 session이라는 의미 단위를 깨뜨려 중복 집계나 상태 불일치 문제를 유발할 수 있음
- 분석 및 Feature 생성에서는 개별 이벤트보다 **session 단위가 더 의미 있는 기준**

**기대 효과**
- 정합성 판단 기준을 Silver 레이어로 고정
- 장애 발생 시 **session 단위 복구 전략 적용 가능**
<br>
<br>

### 6.3. Iceberg Snapshot 기반 증분 처리 / 전체 재처리 분기

**결정**
- **이전 처리 지점의 snapshot과 현재 테이블 snapshot 간의 관계를 기준**으로 처리 전략을 선택
  - 관계가 유지되는 경우 : 증분 처리
  - 관계가 깨진 경우 : 전체 재처리

**배경**
- 증분 처리만을 전제로 한 파이프라인은 과거 데이터 정정, backfill 상황에서 정합성 리스크가 큼
- 반대로 매 배치마다 전체 재처리는 비용·시간 측면에서 비효율적

**기대 효과**
- **증분 처리의 효율성과 전체 재처리의 안정성**을 동시에 고려
- 재처리 전략을 운영자 판단이 아닌 **구조적 기준**으로 분기

> 이 판단을 명확히 하기 위해 **Apache Iceberg**의 snapshot 메타데이터를 활용했습니다.
<br>
<br>

### 6.4. Silver 사전 검증 기반 Gold 실행 제어

**결정**
- Gold 집계 작업 실행 전에 Silver 레이어의 적재 상태를 사전 검증
- 조건을 만족하지 않을 경우 Gold 실행을 중단

**배경**
- Silver 데이터가 불완전한 상태에서 Gold 집계를 수행하면 잘못된 Feature가 생성되고, 이후 복구 비용이 커짐
- Gold는 최종 결과물이기 때문에, **입력 데이터의 신뢰성 보장이 필수**

**기대 효과**
- 불완전한 데이터 기반 Feature 생성 방지
- 파이프라인 실패 지점을 **Silver 단계에서 조기에 차단**
<br>
<br>

### 6.5. 레이어별 의미 단위를 기준으로 한 복구 전략

**결정**
- 장애 발생 시, 레이어별 의미 단위를 기준으로 복구 전략을 분리
  - Streaming(Bronze) : checkpoint 기반 재시작
  - Silver : session 단위 재계산
  - Gold : 집계 파티션 단위(datetime) 재실행

**배경**
- 모든 장애를 동일한 단위로 복구할 경우, 각 레이어의 데이터 의미 단위와 복구 단위가 어긋나서 부분 상태에 의존한 계산이나 중복 반영으로 정합성 문제가 발생할 수 있음
- **각 레이어는 서로 다른 책임과 의미 단위를 가짐**

**기대 효과**
- **복구 기준이 명확**한 파이프라인
- 장애 대응 시 운영자 판단 부담 감소
<br>
<br>
<br>

## 7. 데이터 모델 (Data Model)

본 프로젝트의 데이터 모델은 **각 레이어(Bronze / Silver / Gold)가 서로 다른 책임과 의미 단위를 갖도록 설계**되었습니다.
<br>
<br>
<br>

### 7.1. Bronze (Raw Events)
- Kafka로부터 수집한 유저 행동 이벤트 원본
- 이벤트 timestamp를 기준으로 date 컬럼만 추가된 **append-only 데이터**
- 필요 시 **상위 레이어를 재처리할 수 있는 기준점** 역할
<br>
<br>

### 7.2. Silver (Cleansed & Sessionized Events)
- 이벤트를 session 단위로 재구성하여 **의미 단위를 명확히 정의**
- 중복 제거, null 처리, 타입 캐스팅 등 **정합성 관련 주요 정제 로직**을 수행
- 정합성 이슈 발생 시 **session 단위로 재계산이 가능하도록** 설계
<br>
<br>

### 7.3. Gold (Feature Tables)
- Silver 데이터를 기반으로 생성된 Feature 및 집계 결과
- Feature 목적에 맞는 집계 단위로 구성 (예: 국가, 사용자, 콘텐츠, 플랫폼 등)
- 입력 데이터(Silver)의 상태가 보장된 경우에만 **Feature 생성**
<br>

### 이 데이터 모델이 가지는 의미

이 데이터 모델을 통해:
- 정합성 관련 주요 정제 로직을 Silver 레이어에 집중
- 재처리는 Bronze 또는 Silver 기준으로 수행
- Gold는 검증된 결과만을 담는 레이어

로 역할이 명확히 분리됩니다.


이는 파이프라인 장애나 데이터 오류 발생 시, 문제 원인과 복구 범위를 빠르게 판단할 수 있도록 합니다.
<br>
<br>
<br>
<br>

## 8. 실행 방법 (How to Run)

본 프로젝트는 `Kafka 기반 이벤트 생성 → Streaming 적재 → Batch 처리` 흐름을 단계적으로 실행할 수 있도록 구성되어 있습니다.

아래는 **테스트 환경 기준의 실행 흐름**입니다.
<br>
<br>
<br>

### 8.1. Kafka 이벤트 생성 (Simulator)

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

### 8.2. Airflow 설정 및 파이프라인 실행

Batch 파이프라인(Silver / Gold)은 Airflow DAG을 통해 오케스트레이션됩니다.
<br>
<br>

#### Airflow Connection 설정
Airflow UI에서 아래 Connection들을 사전에 등록합니다.

- **spark_default**
  - Spark Submit을 통한 Batch Job 실행용
- **aws_default**
  - Iceberg 테이블이 저장된 S3 Object Storage 접근용
- **slack_webhook**
  - DAG 실패 알림용 (선택)

> Connection 상세 값은 환경(Spark 실행 모드, AWS 계정 credential, Slack webhook 설정)에 따라 달라질 수 있습니다.
<br>

#### DAG 실행 순서

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
