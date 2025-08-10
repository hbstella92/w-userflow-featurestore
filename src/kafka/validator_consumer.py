import json, signal, sys
from collections import defaultdict
from datetime import datetime
from dateutil import parser as dtparser
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import uuid

KAFKA_TOPIC = "webtoon_user_events_v2"

schema_registry_conf = {
    "url": "http://localhost:28081"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": f"validator-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "key.deserializer": StringDeserializer(),
    "value.deserializer": avro_deserializer
}


stats = defaultdict(int)
state = {}

consumer = DeserializingConsumer(CONSUMER_CONFIG)
consumer.subscribe([KAFKA_TOPIC])



def on_exit(*_):
    print("\n=== Validation Summary ===")
    for k, v in sorted(stats.items()):
        print(f"{k} : {v}")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, on_exit)
signal.signal(signal.SIGTERM, on_exit)


if __name__ == "__main__":
    print("Start validation! (Ctrl + C to exit)")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("ERROR :", msg.error()); continue
        
        key = msg.key() if msg.key() else None
        event = msg.value()
        session_id = event["session_id"]

        cur_state = state.get(session_id)
        if cur_state is None:
            cur_state = state[session_id] = {
                "user_id": event["user_id"],
                "country": event["country"],
                "platform": event["platform"],
                "device": event["device"],
                "browser": event["browser"],
                "webtoon_id": event["webtoon_id"],
                "episode_id": event["episode_id"],
                "last_event_type": None,
                "last_event_time": None,
                "last_scroll_ratio": 0.0,
                "last_scroll_count": 0,
                "last_dwell_ms": 0,
                "seen_enter": False,
                "seen_terminate": False
            }
        
        # 1. 불변 필드 검증
        invariants = ["user_id", "country", "platform", "device", "browser", "webtoon_id", "episode_id"]
        for field in invariants:
            if event[field] != cur_state[field]:
                stats[f"invariant_violation.{field}"] += 1

        # 2. 순서 검증
        event_type = event["event_type"]
        if event_type == "enter":
            if cur_state["seen_enter"]:
                stats["duplicate_enter"] += 1
            cur_state["seen_enter"] = True
        elif event_type == "scroll":
            if not cur_state["seen_enter"]:
                stats["scroll_before_enter"] += 1
            if cur_state["seen_terminate"]:
                stats["scroll_after_terminate"] += 1
        elif event_type in ("complete", "exit"):
            if not cur_state["seen_enter"]:
                stats["terminate_before_enter"] += 1
            if cur_state["seen_terminate"]:
                stats["duplicate_terminate"] += 1
            cur_state["seen_terminate"] = True

        # 3. 단조 증가 검증
        now = dtparser.isoparse(event["timestamp"])
        if cur_state["last_event_time"] and now < cur_state["last_event_time"]:
            stats["time_regression"] += 1
        cur_state["last_event_time"] = now

        if event["scroll_ratio"] < cur_state["last_scroll_ratio"]:
            stats["scroll_ratio_decrese"] += 1
        cur_state["last_scroll_ratio"] = event["scroll_ratio"]

        if event["scroll_event_count"] < cur_state["last_scroll_count"]:
            stats["scroll_count_decrese"] += 1
        cur_state["last_scroll_count"] = event["scroll_event_count"]

        if event["dwell_time_ms"] < cur_state["last_dwell_ms"]:
            stats["dwell_ms_decrese"] += 1
        cur_state["last_dwell_ms"] = event["dwell_time_ms"]

        # 4. 종료 이후 추가 이벤트 방지
        if cur_state["seen_terminate"] and event_type not in ("complete", "exit"):
            stats["post_terminate_events"] += 1
