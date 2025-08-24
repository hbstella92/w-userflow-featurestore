import json
import time
import uuid
import random
import argparse
from datetime import datetime, timedelta, timezone

from dateutil.tz import gettz
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA_TOPIC = "webtoon_user_events_v2"

SCROLL_MIN, SCROLL_MAX = 1, 8
SLEEP_BETWEEN_EVENTS = 1                    # (단위: sec)

SR_URL = "http://localhost:28081"
sr_client = SchemaRegistryClient({"url": SR_URL})
value_schema = open("schemas/webtoon_user_events_v1.avsc").read()
value_serializer = AvroSerializer(sr_client, value_schema)

PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,             # 재시도 상황에서도 중복 없이, 순서 보존되게 전송 (시퀀스 번호로 중복 방지함)
    "acks": "all",
    "linger.ms": 0,                         # 지정한 시간동안 배치 전송
    "retries": 3,
    "partitioner": "murmur2",
    # "key.serializer": StringSerializer("utf_8"),
    # "value.serializer": value_serializer
}

fake = Faker()
KST = gettz("Asia/Seoul")

PLATFORMS = ["web", "android", "ios"]
DEVICES = ["mobile", "pc", "tablet"]
BROWSERS = ["safari", "chrome", "firefox", "edge", "whale"]
NETWORKS = ["wifi", "4g", "5g", "offline"]



# 한 세션동안 불변의 컨텍스트
def make_session_profile(user_id: int | None = None):
    return {
        "user_id": user_id if user_id is not None else fake.random_int(min=1, max=5000),
        # "country": fake.country_code(),
        "country": "KR",
        "platform": random.choice(PLATFORMS),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS)
    }


# 한 세션은 하나의 (wt_id, ep_id)에 묶임
def make_content(webtoon_id: str | None = None, episode_id: str | None = None):
    return {
        "webtoon_id": webtoon_id if webtoon_id is not None else f"webtoon_{fake.random_int(min=1, max=10)}",
        "episode_id": episode_id if episode_id is not None else f"ep_{fake.random_int(min=1, max=30)}"
    }


def make_base_event(session_id: str, t: datetime, profile: dict, content: dict, network: str):
    return {
        "user_id": profile["user_id"],
        "webtoon_id": content["webtoon_id"],
        "episode_id": content["episode_id"],
        "session_id": session_id,
        "timestamp": t.astimezone(timezone.utc).isoformat(),
        # "local_timestamp": t.astimezone(?).isoformat(timespec="seconds"),
        "local_timestamp": t.astimezone(KST).isoformat(timespec="seconds"),
        "country": profile["country"],
        "platform": profile["platform"],
        "device": profile["device"],
        "browser": profile["browser"],
        "network_type": network
    }


def make_enter_event(session_id: str, t: datetime, profile: dict, content: dict, network: str):
    e = make_base_event(session_id, t, profile, content, network)
    e.update({
        "event_type": "enter",
        "scroll_ratio": 0.0,
        "scroll_event_count": 0,
        "dwell_time_ms": 0
    })
    return e


def make_scroll_event(session_id: str, t: datetime, profile: dict, content: dict, network: str,
                      ratio: float, count: int, dwell_ms: int):
    e = make_base_event(session_id, t, profile, content, network)
    e.update({
        "event_type": "scroll",
        "scroll_ratio": round(ratio, 3),
        "scroll_event_count": count,
        "dwell_time_ms": dwell_ms
    })
    return e


def make_terminate_event(session_id: str, t: datetime, profile: dict, content: dict, network: str,
                         is_complete: bool, ratio: float, count: int, dwell_ms: int):
    e = make_base_event(session_id, t, profile, content, network)
    e.update({
        "event_type": "complete" if is_complete else "exit",
        "scroll_ratio": round(ratio, 3),
        "scroll_event_count": count,
        "dwell_time_ms": dwell_ms
    })
    return e


def build_session_events(session_id: str, profile: dict, content: dict, out_of_order_prob: float):
    events = []
    t = datetime.now(timezone.utc)
    network = random.choice(NETWORKS)

    # 1) ENTER
    enter_event = make_enter_event(session_id, t, profile, content, network)
    events.append(enter_event)

    # 2) SCROLL
    n_scrolls = fake.random_int(min=SCROLL_MIN, max=SCROLL_MAX)
    ratio = 0.0
    count = 0
    dwell_ms = 0

    for i in range(n_scrolls):
        if random.random() < 0.1:
            network = random.choice(NETWORKS)

        delta_ms = fake.random_int(min=500, max=5000)
        dwell_ms += delta_ms
        t += timedelta(milliseconds=delta_ms)

        ratio = min(1.0, ratio + random.uniform(0.05, 0.25))
        count += 1

        scroll_event = make_scroll_event(session_id, t, profile, content, network,
                                         ratio, count, dwell_ms)
        events.append(scroll_event)

    # 3) COMPLETE 혹은 EXIT
    delta_ms = fake.random_int(min=500, max=5000)
    dwell_ms += delta_ms
    t += timedelta(milliseconds=delta_ms)

    is_complete = random.random() < 0.6
    if is_complete and ratio < 1.0:
        ratio = min(1.0, max(1.0, ratio + random.uniform(0.01, 0.2)))
    
    terminate_event = make_terminate_event(session_id, t, profile, content, network,
                                           is_complete, ratio, count, dwell_ms)
    events.append(terminate_event)

    # out-of-order (enter와 첫 scroll의 순서를 바꾸는 정도로만 제한)
    if out_of_order_prob > 0 and random.random() < out_of_order_prob and len(events) >= 3:
        events[0], events[1] = events[1], events[0]
    
    return events


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        key = msg.key().decode() if msg.key() else None
        print(f"Message delivered to {msg.topic()} [partition={msg.partition()} offset={msg.offset()} key={key}]")


def parse_args():
    ap = argparse.ArgumentParser(description="Faker-based Kafka Producer")

    ap.add_argument("--sessions", type=int, default=10, help="Number of sessions")
    ap.add_argument("--ooo-prob", type=float, default=0.0, help="out-of-order probability")
    ap.add_argument("--binge", action="store_true", help="Enable binge mode")
    ap.add_argument("--binge-sessions", type=int, default=5, help="Number of sessions during binge mode")
    
    return ap.parse_args()


def main():
    args = parse_args()

    # producer = SerializingProducer(PRODUCER_CONFIG)
    producer = Producer(PRODUCER_CONFIG)

    if not args.binge:
        for _ in range(args.sessions):
            session_id = str(uuid.uuid4())
            profile = make_session_profile()
            content = make_content()
            
            session_events = build_session_events(session_id, profile, content, args.ooo_prob)

            for event in session_events:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    # key=session_id,
                    # value=event,
                    key=str(session_id).encode("utf-8"),
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                    on_delivery=delivery_report
                )
                producer.poll(0)                        # 콜백 처리와 I/O 처리를 위한 이벤트 루프 돌리기
                time.sleep(SLEEP_BETWEEN_EVENTS)
        
        producer.flush()                                # 큐에 남은 메세지를 모두 전송 완료할 때까지 대기 (유실 없이 마무리하기 위함)
        print("DONE")
        return
    
    for _ in range(args.sessions):
        profile = make_session_profile()

        for i in range(args.binge_sessions):
            session_id = str(uuid.uuid4())
            content = make_content()

            session_events = build_session_events(session_id, profile, content, args.ooo_prob)

            for event in session_events:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    # key=session_id,
                    # value=event,
                    key=str(session_id).encode("utf-8"),
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                    on_delivery=delivery_report
                )
                producer.poll(0)
                time.sleep(SLEEP_BETWEEN_EVENTS)
            
            time.sleep(random.uniform(0.5, 2.0))

    producer.flush()
    print("DONE")
    return


if __name__ == "__main__":
    main()