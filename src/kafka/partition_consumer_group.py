import json, os
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

KAFKA_TOPIC = "webtoon_user_events_v2"

schema_registry_conf = {
    "url": "http://localhost:28081"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer(),
    "value.deserializer": avro_deserializer
}

consumer = DeserializingConsumer(CONSUMER_CONFIG)
consumer.subscribe([KAFKA_TOPIC])

print(f"Start!")

try:
    while True:
        msg = consumer.poll(1)

        if not msg:
            continue
        if msg.error():
            print("ERROR :", msg.error()); continue
        
        key = msg.key() if msg.key() else None
        value = msg.value()
        print(f"topic: {msg.topic()} partition = {msg.partition()} offset = {msg.offset()} key = {key} event = {value['event_type']}")
finally:
    consumer.close()