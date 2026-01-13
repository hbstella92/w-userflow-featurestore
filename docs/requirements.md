# 📘 w-userflow-featurestore 요구사항 명세서

## 1. 프로젝트 개요

본 프로젝트는 글로벌 웹툰 플랫폼 사용자들의 실시간 행동 로그를 기반으로
완독률, 이탈 패턴, 스크롤 행동 등을 분석하여
추천 시스템에서 활용할 수 있는 **Feature Store**를 구축하는 것을 목표로 한다.

Kafka 기반 실시간 이벤트 수집, PySpark 상태 관리 로직, 실시간/배치 Feature Store 운영을 통해
추천 품질 향상, UX 최적화, 콘텐츠 품질 관리에 활용 가능한 데이터 인프라를 구성한다.

**중점 사항:**

- 글로벌 사용자 행동 분석 (국가/장르/시간대 대응)
- 실시간 + 배치 feature 이중 운영
- 추천 API에서 직접 사용 가능한 Feature Store 제공
- A/B 테스트 실험 데이터 포함
- 확장 가능한 데이터 파이프라인 설계

---

## 2. 문제 정의

현재 글로벌 웹툰 플랫폼에서는 실시간 사용자 행동 분석 및 feature 생성 인프라가 부재하여
추천 시스템 최적화와 사용자 경험 개선이 어렵다.

**문제점:**
- 최신 행동 정보를 추천 시스템에 반영하지 못함
- 실시간 사용자 이탈 탐지 불가
- 국가/시간대별 사용자 흐름 차이를 고려한 추천 불가
- UX 실험(A/B 테스트) 효과 측정 불가

**원인:**
- 배치 위주의 로그 분석
- 사용자 행동 데이터와 콘텐츠 메타데이터 결합 미흡
- 품질 검증/자동화 부재

**해결 방향:**
- 실시간 사용자 행동 이벤트 수집 및 처리
- 상태 기반 이탈 탐지 및 feature 생성
- 추천 시스템 용 Feature Store 제공
- 품질 검증 및 자동화 포함한 파이프라인 구축

---

## 3. 시스템 목표

| 목표 항목 | 목표 |
|-----------|------|
| 실시간 feature 생성 지연 | 5초 이내 (Kafka -> Feature Store) |
| feature 제공 단위 | session_id, user_id, episode_id |
| feature 개수 | 최소 10종 이상의 추천 feature |
| 품질 검증 항목 | Null/중복/순서 오류/범위 오류 0건 |
| 배치 feature 집계 주기 | 1시간 단위 |

**구체적 목표:**
1. 실시간 행동 feature 자동 생성 및 추천 시스템 제공
2. 배치 기반 장기 feature 관리
3. 상태 기반 사용자 흐름 분석 (이탈/완독 탐지)
4. 국가/장르/시간대 구분 feature 생성
5. A/B 테스트 실험 분석 지원
6. 품질 검증 및 파이프라인 자동화

---

## 4. 핵심 기능 요건

### 1. 사용자 이벤트 실시간 수집
- Kafka 기반 이벤트 수집 (enter, scroll, complete, exit)
- 국가, 디바이스, 플랫폼 정보 포함
- A/B 테스트 그룹 태깅 포함 (experiment_group, experiment_name)

### 2. 상태 기반 사용자 흐름 분석
- PySpark Structured Streaming
- 세션 상태 관리 (flatMapGroupsWithState)
- 완독/이탈 판별 및 feature 생성

### 3. 추천 Feature Store 제공
- 실시간 Feature Store (PostgreSQL)
- 배치 Feature Store (Hive)
- session/user/episode 단위 feature 제공

### 4. Feature 모델링
- session -> user / episode 단위 집계

---

### 5. 파이프라인 자동화 및 모니터링 (Airflow)
- feature 집계/품질 검증 자동화
- 파이프라인 상태 모니터링

---

### 7. 확장성 및 유지보수성
- 신규 feature 추가 용이
- 국가/장르/실험그룹 등 확장 지원
- Kubernetes 기반 클라우드 인프라 확장 가능