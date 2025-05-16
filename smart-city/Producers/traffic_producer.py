import os
import time
import random
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker('en_IN')

# Define Avro schema
schema_str = json.dumps({
    "type": "record",
    "name": "TrafficData",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "road", "type": "string"},
        {"name": "congestion", "type": "int"},
        {"name": "timestamp", "type": "string"},
        {"name": "coordinates", "type": {
            "type": "record",
            "name": "Coordinates",
            "fields": [
                {"name": "lat", "type": "double"},
                {"name": "lon", "type": "double"}
            ]
        }}
    ]
})

# Schema Registry configuration
schema_registry_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer configuration
producer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

delhi_roads = [
    "MG Road", "India Gate Circle", "Barapullah Elevated Road",
    "Connaught Place", "NH-48", "Outer Ring Road"
]

def generate_traffic_data():
    road = random.choice(delhi_roads)
    return {
        "sensor_id": f"TR_{road[:3].upper()}_{random.randint(1000,9999)}",
        "road": road,
        "congestion": random.randint(0, 100),
        "timestamp": fake.iso8601(tzinfo=None),
        "coordinates": {
            "lat": 28.6139 + random.uniform(-0.02, 0.02),
            "lon": 77.2090 + random.uniform(-0.02, 0.02)
        }
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

try:
    while True:
        data = generate_traffic_data()
        producer.produce(
            topic='traffic-data',
            key=data['sensor_id'],
            value=data,
            on_delivery=delivery_report
        )
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()