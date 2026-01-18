# **w-userflow-featurestore**
<br>

## 1. 프로젝트 개요

본 프로젝트는<br>
**실시간 유저 행동 이벤트를 수집하면서도,<br>
데이터 정합성과 신뢰성을 고려한 Feature 생성 파이프라인 구조**를 설계/구현한 개인 프로젝트입니다.

Kafka 기반 스트리밍 수집, Spark Structured Streaming을 활용한 집계 및 적재,<br>
Apache Iceberg를 중심으로 한 Bronze-Silver-Gold 메달리온 아키텍처 설계,<br>
Airflow 기반 배치 오케스트레이션과 Trino + Grafana를 통한 Feature 조회까지<br>
기존에 다뤄왔던 도메인과는 다른 **서비스형 이벤트 데이터 처리 경험**을 쌓기 위해 진행했습니다.
<br>
<br>
<br>

## 2. 프로젝트 목적

이 프로젝트는 다음과 같은 목적에서 출발했습니다.<br>

1. **도메인이 다른 데이터 경험**
    - 웹툰 서비스와 같은 실제 서비스 도메인의 유저 행동 데이터
    - "행동 이벤트" 중심 데이터 모델링 경험

2. **최신 데이터 스택 활용**
    - Kafka + Spark Structured Streaming 기반 이벤트 수집
    - Apache Iceberg 기반 레이크 테이블 관리
    - Trino + Grafana를 통한 Feature 조회 및 시각화
<br>
<br>

## 3. Data Flow
```
Simulated User Events
↓
Kafka
↓
Spark Structured Streaming
↓
Iceberg (Bronze: Raw Events)
↓
Batch Processing (Airflow)
↓
Iceberg (Silver: Cleansed & Sessionized Events)
↓
Iceberg (Gold: Feature Tables)
↓
Trino
↓
Grafana
```
<br>
<br>

## 4. Architecture

### 주요 구성 요소
- **Kafka**<br>
    유저 행동 이벤트 수집

- **Spark Structured Streaming**<br>
    실시간 유저 이벤트를 처리하여 Bronze 레이어에 적재<br>
    Airflow DAG에서 trigger하여 실행

- **Apache Iceberg + AWS S3**<br>
    대규모 이벤트 및 Feature 테이블 저장<br>
    Snapshot 기반 증분 처리 및 재처리 지원

- **Apache Airflow**<br>
    Silver / Gold 배치 Spark Job 파이프라인 오케스트레이션<br>
    데이터 상태 기반 실행 제어

- **Trino**<br>
    Feature 조회용 쿼리 엔진

- **Grafana**<br>
    Feature 지표 시각화
<br>
<br>

## 5. Layer별 설계

### Bronze - Raw Events
- Kafka 이벤트를 Spark Structured Streaming으로 수집
- 최소한의 가공(datetime 컬럼 추가)만 수행
- 30초 단위 micro-batch로 Iceberg 테이블에 append

### Silver - Cleansed & Sessionized Events
- Bronze 테이블을 입력으로 사용
- 다음 작업 수행
    - 중복 이벤트 제거(deduplication)
    - not null 처리 및 타입 캐스팅
    - session 단위로 이벤트를 묶어 세션 단위 레코드 생성
    - 완독 / 이탈 / 타임아웃 등 session 상태 판별

<br>

**설계 배경**<br>
서비스 도메인에서는 개별 이벤트보다<br>
**session 단위가 사용자 행동 분석에 더 의미 있는 단위**이기에,<br>
Gold 집계 이전에 session 단위로 데이터를 정리했습니다.
<br>

### Gold - Feature Tables
- Silver 레이어를 기반으로 Feature 생성
- Session 단위 유저 행동 Feature 및 집계 지표 적재
- 분석 및 시각화에 사용
<br>
<br>

## 6. Iceberg Snapshot 기반 증분 처리 설계

### 문제
- 매 배치마다 전체 데이터를 읽는 방식은 비효율적
- 무조건 증분 처리만 적용할 경우 정합성 리스크 존재

### 해결 방식
- 이전 DAG run에서 처리 완료된 snapshot ID를 Airflow XCom/Variable에 저장
- 현재 DAG run 기준 최신 snapshot ID 조회
- 두 snapshot이 **조상(ancestor)-자손(descendant) 관계인지 확인**
    - 관계가 맞을 경우: incremental read
    - 관계가 아닐 경우: full read

이 방식을 통해<br>
증분 처리의 효율성과 전체 재처리 안전성을 함께 확보했습니다.
<br>
<br>
<br>

## 7. Silver 사전 검증 기반 Gold 실행 제어

Gold Feature 집계 전에<br>
**Silver 데이터가 정상적으로 적재되었는지 사전 검증하는 task**를 추가했습니다.

- Silver 테이블의 전일 적재 결과(data file 수)를 기준으로 적재 완료 여부 확인
- 조건을 만족하지 않으면 Gold 집계 task 실행 중단

이를 통해<br>
불완전한 Silver 데이터를 기반으로 Feature 지표가 생성되는 상황을 방지했습니다.
<br>
<br>
<br>

## 8. 기술 선택 이유
- **Kafka**: 대량 이벤트 수집
- **Spark Structured Streaming**: 스트리밍과 배치 연계에 용이
- **Apache Iceberg**: Snapshot 기반 증분 처리 및 대규모 테이블 관리<br>
  (향후 Time Travel 등 snapshot 메타데이터 활용 측면에서 확장성이 높은 테이블 포맷으로 판단하여 채택)
- **Airflow**: DAG 기반 task 파이프라인 제어
- **Trino**: Iceberg 기반 Feature를 Grafana와 연동하기 위한 쿼리 엔진
- **Grafana**: Feature 지표 시각화
<br>
<br>

## 9. 주요 트러블슈팅 사례

<br>
<br>

## 10. 한계점
* 실시간 Feature 제공 불가 (배치 기반)
* Snapshot 메타데이터 관리 복잡성 증가
* 데이터 규모 증가 시 Silver → Gold 배치 비용 증가
<br>
<br>

## 11. 정리

이 프로젝트는<br>
**새로운 도메인과 최신 기술 스택을 실제로 사용해보며,<br>
서비스형 이벤트 데이터 처리 전반을 경험하는 것**을 목표로 했습니다.

* 실시간/배치 혼합 처리 구조에 대한 설계 판단
* 데이터 정합성 이슈를 고려한 파이프라인 구조 설계
* 증분 처리와 재처리 간 trade-off 인식
* 데이터 상태 기반 파이프라인 실행 제어

를 함께 고민한 프로젝트입니다.