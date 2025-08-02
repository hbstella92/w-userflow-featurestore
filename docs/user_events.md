# 🧾 사용자 이벤트 스키마 정의서

## 1. 개요
본 문서는 w-userflow-featurestore 프로젝트에서 사용되는
사용자 행동 이벤트 로그의 스키마 및 처리 규칙을 정의한다.

웹툰 플랫폼 사용자들의 스크롤, 이탈, 완독 등의 행동 정보를
Kafka 기반 실시간 스트리밍으로 수집하며,
이벤트는 세션 상태 추적, 완독/이탈 탐지, 추천 feature 생성의 핵심 입력 데이터로 활용된다.

수집 목적:
- 실시간 상태 기반 완독/이탈 탐지
- 추천 시스템 용 행동 feature 생성 (세션/사용자/에피소드 단위)
- 글로벌 사용자 행동 패턴 분석 (국가/시간대 구분)
- A/B 테스트 실험 결과 분석을 위한 그룹 태깅

모든 이벤트는 Kafka topic인 `webtoon_user_events`로 발행되며,
콘텐츠 메타데이터(장르, 태그 등)는 별도 테이블로 관리한다.

---

## 2. 이벤트 타입

| 이벤트명 | 설명 | 필수 여부 |
|------------|-------------------------------------------|-----------|
| enter | 웹툰 에피소드 진입 시 발생 | O |
| scroll | 스크롤 시 발생 (scroll_ratio 포함) | O |
| complete | 콘텐츠 끝까지 본 경우 발생 (완독 판정용) | X |
| exit | 페이지 이탈 또는 세션 종료 시 발생 | O |

이벤트 흐름:
- **완독:** enter -> scroll -> complete
- **이탈:** enter -> scroll -> exit
- **비정상:** enter -> exit (스크롤 없음) -> 무시

---

## 3. 사용자 행동 이벤트 스키마

```json
{
    "user_id": "int",
    "webtoon_id": "string",
    "episode_id": "string",
    "session_id": "string",
    "timestamp": "string (ISO 8601)",           // 이벤트 발생 시각 (UTC)
    "local_timestamp": "string (ISO 8601)",     // 현지 시간 (국가별 시간대 분석용)
    "event_type": "string",                     // enter, scroll, complete, exit
    "country": "string",                        // ISO 3166-1 alpha-2
    "platform": "string",                       // 네이버웹툰 / 라인웹툰 등 글로벌 플랫폼 구분
    "device": "string",                         // mobile / pc / tablet
    "browser": "string",                        // safari / chrome / firefox / edge / whale
    "network_type": "string",                   // wifi / 4g / 5g / offline

    "scroll_ratio": "float (0.0~1.0)",          // scroll 이벤트에만 포함
    "scroll_event_count": "int",                // 세션 내의 스크롤 이벤트 발생 횟수
    "dwell_time_ms": "long",                     // 세션 내의 총 머문 시간 (종료 시점에서 채워짐)

    "recommendation_source": "string",          // (추천에 의해 진입한 경우) 추천 알고리즘 타입
    "user_segment": "string",                   // 신규/기존/휴면 등 마케팅 구분

    "experiment_group": "string",               // A/B 테스트 그룹
    "experiment_name": "string"                 // A/B 테스트 이름
}
```

---

## 4. 완독 및 이탈 판별 로직

각 세션의 이벤트 흐름을 기반으로 완독(Completion) 및 이탈(Exit) 여부를 판별한다.
이 판별 결과는 session_features 테이블의 is_exit_tag, completion_rate feature 생성에 사용된다.

### ✅ 완독(Completion) 판별 조건
- 필수 이벤트: `enter -> scroll -> complete`
- 최종 스크롤 이벤트에서 `scroll_ratio >= 1.0`
- complete 이벤트가 enter 이후 5분(300초) 이내 발생해야 유효

---

### ❌ 이탈(Exit) 판별 조건
- 필수 이벤트: `enter -> scroll -> exit`
- `scroll_ratio < 1.0`
- complete 이벤트가 존재하지 않음
- exit 이벤트가 enter 이후 10분(600초) 이내 발생해야 유효

---

### 🔄 무효/예외 처리
- `enter -> exit` (스크롤 이벤트 없는 경우): 제외
- 중복 이벤트 발생 시: 가장 먼저 발생한 유효한 흐름만 분석 대상
- 이벤트 순서 오류 발생 시: 제외
- complete, exit 모두 누락된 세션: 미완성 세션으로 처리

---

## 5. 이벤트 흐름 및 처리 규칙

모든 이벤트는 session_id 단위로 관리하며, timestamp 기준으로 정렬 후 처리한다.

### ✅ 세션 흐름 처리 규칙
- enter 이벤트로 세션 시작
- scroll 이벤트는 중복 발생 가능
- complete 또는 exit 발생 시 세션 종료
- 10분 타임아웃 후 자동 종료

### ✅ 세션 종료 후 수행 작업
- 완독/이탈 판별 적용
- session_features에 다음 정보 저장:
    - session_id
    - user_id
    - episode_id
    - is_exit_tag (boolean)
    - completion_rate
    - dwell_time_ms
    - scroll_event_count

---

### ⚠️ 예외 처리 규칙
- scroll 없이 바로 exit한 경우: 분석 제외
- 이벤트 순서 역전 발생 시: 해당 세션 제외
- 세션 미완성 상태: dwell_time_ms, scroll_event_count 집계 불가로 처리

---

## 6. 활용 방안
- PySpark 상태 관리 로직에서 세션별 상태를 추적 후, 완독/이탈 여부 판별
- session_features 테이블로 feature 생성
- Postgres/Hive Feature Store에 저장 후, 추천 시스템에 제공
- A/B 테스트 분석에서 experiment_group 별 행동 차이 분석
- 국가/플랫폼/시간대/디바이스 별 사용자 행동 흐름 분석 및 리포트 생성