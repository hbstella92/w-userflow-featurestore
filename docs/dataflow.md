# 📊 데이터 플로우 설계 문서 (w-userflow-featurestore)

## 1. 시스템 구성 개요

w-userflow-featurestore 프로젝트는 글로벌 웹툰 플랫폼에서 발생하는 실시간 사용자 행동 로그를 수집/처리하여
추천 시스템에서 바로 활용할 수 있는 사용자/콘텐츠 feature를 자동으로 생성/제공하는 데이터 파이프라인 구축을 목표로 한다.

웹툰 에피소드의 **완독률, 이탈률, 스크롤 행동**을 실시간으로 분석하고,
각 사용자, 세션, 에피소드 단위로 **몰입도 지표 feature**를 추출하여 **Feature Store** 형태로 구성한다.
이를 통해 추천 시스템이 최신 사용자 행동을 반영한 개인화 추천을 수행할 수 있도록 지원한다.

### 주요 구성 요소:
- **Kafka**: 사용자 행동 이벤트 실시간 수집
- **PySpark Structured Streaming**: 상태 기반 실시간 처리 및 feature 전처리
- **PostgreSQL / Hive**: 실시간 및 배치 feature store 구축
- **dbt**: SQL 기반 feature 모델링 및 집계
- **Great Expectations**: 데이터 품질 검증
- **Airflow**: 배치 파이프라인 및 품질 검증 자동화

이 시스템은 **글로벌 사용자를 대상으로 실시간/배치 혼합 파이프라인**을 운영하며,
국가별/장르별/시간대별 사용자 행동 패턴을 기반으로 추천 시스템 최적화 및 콘텐츠 몰입도 개선을 위한 데이터 인프라 역할을 수행한다.

---

## 2. 전체 데이터 흐름 요약

본 시스템은 실시간 사용자 행동 이벤트로부터 몰입도 지표를 포함한 feature를 추출하고,
추천 시스템이 활용할 수 있는 Feature Store로 제공하는 데이터 파이프라인이다.

### 전체 데이터 흐름:

1. **사용자 이벤트 수집**
    - 웹툰 플랫폼에서 발생하는 사용자 행동 이벤트 (enter, scroll, complete, exit)를 Kafka로 실시간 수집
    - 국가, 디바이스, 접속 플랫폼 등의 부가 정보를 함께 수집

2. **실시간 처리 (PySpark Structured Streaming)**
    - Kafka로부터 수집된 이벤트를 session_id 단위로 그룹화하여 상태 관리
    - 사용자/세션/에피소드 단위의 실시간 행동 feature를 계산
    - 완독 여부, 이탈 여부, 스크롤량, 머문 시간 등의 feature를 실시간으로 추출

3. **데이터 저장 및 Feature Store 구성**
    - 실시간 feature는 PostgreSQL에 저장 (실시간 Feature Store)
    - 장기 배치 feature는 Hive에 저장 (배치 Feature Store)

4. **Feature 모델링 및 집계 (dbt)**
    - session -> user, episode 레벨로 feature 집계
    - 에피소드 메타데이터(장르, 태그, 평균 별점 등)와 조인하여 추천 feature 생성

5. **품질 검증 (Great Expectations)**
    - 수집된 이벤트 및 생성된 feature에 대한 데이터 품질 검증 수행
    - 검증 실패 시 자동 알림 발생

6. **파이프라인 자동화 (Airflow)**
    - 실시간 처리 모니터링 및 배치 feature 집계 자동화
    - 품질 검증 포함한 DAG 구성

---

## 3. 컴포넌트 별 상세 플로우

### 3.1 사용자 이벤트 수집 (Kafka)

- 웹툰 사용자 행동 이벤트는 Kafka topic으로 실시간 수집된다.
- Python 기반 Kafka Producer가 사용자 이벤트 시뮬레이터 역할을 한다.
- 주요 이벤트 종류: enter, scroll, complete, exit
- 각 이벤트는 session_id 기준으로 Kafka 파티션에 기록된다.
- 주요 필드:
    - user_id, session_id, episode_id, event_type, scroll_ratio, country, device, platform, timestamp 등
- Kafka topic:
    - `webtoon_user_events`
- 데이터 유형: JSON 메세지

---

### 3.2 실시간 처리 (PySpark Structured Streaming)

- Kafka에서 실시간 이벤트를 소비하여 상태 기반 스트림 처리 수행
- **flatMapGroupsWithState**를 활용하여 session_id 기준으로 상태 관리
- 사용자 세션 내에서 완독/이탈 판별 및 feature 추출
- 계산된 feature 예시:
    - is_exit_tag (boolean)
    - completion_rate (0.0 ~ 1.0)
    - dwell_time (ms)
    - scroll_event_count
    - country, device, genre

- 처리된 데이터를 다음으로 전송:
    - PostgreSQL (실시간 feature store)
    - Hive (배치 feature store)

---

### 3.3 데이터 저장 (Feature Store)

#### a. 실시간 Feature Store (PostgreSQL)

- 실시간 추천 시스템에서 활용할 feature 저장소
- 저장 테이블:
    - `session_features`
    - `user_features`
    - `episode_features`
- PostgreSQL은 빠른 조회를 위한 최신 데이터 전용 저장소로 사용

#### b. 배치 Feature Store (Hive)

- 장기 분석 및 추천 모델 학습용 feature 저장소
- Hive를 활용해 대규모 feature 데이터를 저장
- 장기적이고 대용량의 사용자 행동/콘텐츠 데이터를 보관

---

### 3.4 Feature 모델링 및 집계 (dbt)

- dbt를 사용하여 feature store 내 데이터 집계 및 가공
- 주요 작업:
    - session -> user / episode 레벨로 집계
    - episode_metadata와 조인하여 episode_features 생성
- 생성되는 주요 feature:
    - user_avg_completion_rate
    - user_exit_rate
    - episode_avg_scroll_ratio
    - episode_avg_rating
    - current_popularity_rank
    - tags, genre 포함

---

### 3.5 데이터 품질 검증 (Great Expectations)

- Kafka raw event, session_features, dbt 모델링 결과에 대해 데이터 품질 검증 수행
- 주요 검증 항목:
    - 필수 필드 존재 여부
    - 값의 범위 검증 (ex. scroll_ratio ∈ [0.0, 1.0])
    - Null 값 허용 여부
    - feature 값 이상치 검출
- 검증 실패 시 Airflow를 통해 Slack 알림 전송

---

### 3.6 파이프라인 자동화 및 리포트 (Airflow)

- Airflow DAG로 전체 데이터 파이프라인을 스케줄링 및 모니터링
- 자동화 항목:
    - feature 집계 배치 작업
    - 데이터 품질 검증 자동 실행
    - 배치 feature store 데이터 업데이트
    - Slack 알림 및 모니터링

---

## 4. 향후 확장 계획

본 시스템은 웹툰 에피소드 단위의 사용자 완독/이탈 행동을 실시간으로 분석하여 추천 시스템에 활용할 feature를 생성하는 것을 목표로 하지만,
이후 다음과 같은 방향으로 확장 가능성을 고려한다.

---

### 4.1 A/B 테스트 그룹 태깅 및 분석

- 실험 그룹(A/B/C 등)을 사용자 이벤트에 태깅하여 추천 알고리즘 실험 성능을 분석
- 그룹별 이탈률, 완독률 차이를 비교 분석
- 실시간 feature 생성 시 실험 그룹 구분 태그 포함

---

### 4.2 국가/장르별 사용자 흐름 세분화 분석

- 글로벌 서비스 특성을 반영하여, 국가/시간대/장르별로 사용자의 몰입도/이탈 패턴을 분석
- 국가별 추천 최적화 또는 UX 개선에 활용
- 국가/장르별 feature를 feature store에 포함

---

### 4.3 추천 시스템 API 연동

- Postgres 기반 실시간 feature store를 추천 시스템의 API 또는 서빙 레이어에 직접 연동
- 유저 접속 시점에서 최신 세션 feature를 바로 읽어 개인화 추천 제공

---

### 4.4 세션 흐름 리플레이 및 UI 개선 피드백

- 사용자 행동 흐름을 세션 단위로 리플레이하여 UX 개선 자료로 활용
- 특정 UI 변경 후, 이탈/완독 패턴 변화를 실시간으로 모니터링

---

### 4.5 콘텐츠 품질 평가 시스템으로 확장

- 개별 웹툰 작품/에피소드의 몰입도 지표를 "콘텐츠 품질 평가 feature"로 정의
- 신규 웹툰 런칭 후, 초반 이탈률, 완독률을 기반으로 추천 제외/우선 노출 등 품질 관리 지원

---

### 4.6 AI 모델 학습용 feature 로그 생성

- 사용자 행동 기반 feature를 추천 모델 학습용으로 저장 및 제공
- ML Feature Store와 연계하여 대규모 모델 학습 파이프라인 구성 지원

---

### 4.7 인프라 확장

- 실시간 처리 부하 증가 대비 Kafka, Spark 클러스터 오토스케일링 구성
- Redis 기반 feature serving 도입
- Kubernetes 기반 클라우드 인프라 확장 적용