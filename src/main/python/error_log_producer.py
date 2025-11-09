import json
import random
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError, NodeNotReadyError
# import logging
# logging.basicConfig(level=logging.DEBUG)

# Kafka configuration

TOPIC_NAME = 'errorlog-topic'
KAFKA_BROKER = ['localhost:9092']

for attempt in range(1):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER
        )
        print("Connected to Kafka Admin!")
        break
    except NodeNotReadyError as e:
        print(f"Kafka broker not ready, retrying ({attempt+1}/10)...")
        time.sleep(2)
else:
    raise Exception(e)

topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)

try:
    admin_client.create_topics([topic])
    print(f"✅ Topic '{TOPIC_NAME}' created successfully.")
except TopicAlreadyExistsError:
    print(f"ℹ️ Topic '{TOPIC_NAME}' already exists.")
finally:
    admin_client.close()

MICROSERVICES = ["service-A", "service-B", "service-C"]
ERROR_TYPES = ["DB_ERROR", "NETWORK_ERROR", "AUTH_ERROR"]


def generate_error_log(i):
    # error_type = ERROR_TYPES[i % len(ERROR_TYPES)]
    # service = MICROSERVICES[i % len(MICROSERVICES)]

    error_type = random.choice(ERROR_TYPES)
    service = random.choice(MICROSERVICES)

    return {
        "service": service,
        "error_type": error_type,
        "message": "Error occurred",
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')

)

for i in range(100):
    error_log = generate_error_log(i)
    producer.send(topic = TOPIC_NAME, key=str(error_log['error_type']), value=error_log)
    print(f"📤 Sent: {error_log}")
    time.sleep(10)  # Send every 10 seconds

producer.flush()
print("✅ All messages sent.")
