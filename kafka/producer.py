import json
import random
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker
from pytz import timezone

from kafka import KafkaProducer

KAFKA_TOPIC = "webtoon_user_events"

EVENT_TYPES = ["enter", "scroll", "complete", "exit"]

PLATFORMS = ["Naver webtoon", "Line webtoon"]
DEVICES = ["mobile", "pc", "tablet"]
BROWSERS = ["safari", "chrome", "firefox", "edge", "whale"]
NETWORK_TYPES = ["wifi", "4g", "5g", "offline"]

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
previous_timestamp = None


def generate_event(event_type, previous_event_timestamp=None):
    user_id = fake.random_int(min=1000, max=9999)
    webtoon_id = fake.uuid4()
    episode_id = f"ep-{fake.random_int(min=1, max=10)}"
    session_id = str(uuid.uuid4())

    if previous_event_timestamp:
        timestamp_obj = datetime.fromisoformat(previous_event_timestamp)
        timestamp = (timestamp_obj + timedelta(seconds=random.randint(5, 60))).isoformat()
    else:
        timestamp = datetime.utcnow().isoformat()
    timestamp_obj = datetime.fromisoformat(timestamp)
    local_timestamp = (timestamp_obj + timedelta(hours=9)).isoformat()

    event = {
        "user_id": user_id,
        "webtoon_id": webtoon_id,
        "episode_id": episode_id,
        "session_id": session_id,
        "timestamp": timestamp,
        "local_timestamp": local_timestamp,
        "event_type": event_type,
        "country": fake.country_code(),
        "platform": random.choice(PLATFORMS),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "network_types": random.choice(NETWORK_TYPES)
    }

    if event_type == "scroll":
        event["scroll_ratio"] = round(random.uniform(0, 1), 2)
        event["scroll_event_count"] = random.randint(1, 50)

    if event_type in ["complete", "exit"]:
        event["dwell_time_ms"] = random.randint(30000, 600000)

    return event, timestamp


def send_event_to_kafka(event_type, previous_timestamp=None):
    event, new_timestamp = generate_event(event_type, previous_timestamp)
    producer.send(KAFKA_TOPIC, value=event)
    print(f"Sent event : {event}")
    return new_timestamp


if __name__ == "__main__":
    while True:
        event_type = random.choice(EVENT_TYPES)
        previous_timestamp = send_event_to_kafka(event_type, previous_timestamp)
        time.sleep(random.randint(2, 5))
