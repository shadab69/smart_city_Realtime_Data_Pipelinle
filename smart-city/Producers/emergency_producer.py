import os, time, random
from dotenv import load_dotenv
from faker import Faker
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer

load_dotenv()
fake = Faker('en_IN')


#hello babu or kia haal h 


schema_str = """
{
  "type": "record",
  "name": "EmergencyEvent",
  "fields": [
    {"name": "emergency_id", "type": "string"},
    {"name": "type", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "severity", "type": "string"},
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
}
"""

def emergency_dict(obj, ctx):
    return obj

schema_registry_conf = {'url': os.getenv('SCHEMA_REGISTRY_URL')}
schema_registry_conf['basic.auth.user.info'] = f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(schema_registry_client, schema_str, emergency_dict)
string_serializer = StringSerializer('utf_8')

producer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'key.serializer': string_serializer,
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

landmarks = ["Connaught Place", "India Gate", "Red Fort", "Chandni Chowk", "Jama Masjid", "Lotus Temple"]

def generate_emergency():
    return {
        "emergency_id": f"DEL-EMG-{random.randint(1000,9999)}",
        "type": random.choice(["Accident", "Fire", "Medical", "Crime"]),
        "location": random.choice(landmarks),
        "severity": random.choice(["Low", "Medium", "High", "Critical"]),
        "timestamp": fake.iso8601(tzinfo=None),
        "coordinates": {
            "lat": 28.61 + random.uniform(-0.05, 0.05),
            "lon": 77.23 + random.uniform(-0.05, 0.05)
        }
    }

while True:
    record = generate_emergency()
    producer.produce(topic="emergency-data", key=record["emergency_id"], value=record)
    producer.flush()
    time.sleep(random.randint(2, 6))
