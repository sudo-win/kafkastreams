from kafka import KafkaConsumer
import sqlite3
import json

# Kafka configuration
consumer = KafkaConsumer(
    'top-error-topic',                    # Topic name
    bootstrap_servers=['localhost:9092'],
    group_id='sqlite-writer',
    auto_offset_reset='earliest',  # Read from beginning if no offsets
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode('utf-8'),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

import datetime


# Connect to SQLite
conn = sqlite3.connect('/home/r00t/Downloads/kafka_2.13-4.1.0/java-code/kafkastreams/mydatabase.db')
cursor = conn.cursor()

print("Listening for Kafka messages...")

try:
    for msg in consumer:
        key = msg.key
        value =  json.loads(msg.value)

        windowStart = datetime.datetime.fromtimestamp(int(value['windowStart']) / 1000)
        windowEnd = datetime.datetime.fromtimestamp(int(value['windowEnd']) / 1000)
        # print(value['value'])
        value = str(value['value'])
        # topic = msg.topic
        # partition = msg.partition
        # offset = msg.offset
        # timestamp = msg.timestamp


        # Optional: try parsing JSON values
        # try:
        #     json.loads(value)
        # except:
        #     pass  # keep as string if not JSON

        # cursor.execute(
        #     '''
        #     INSERT INTO top_errors (key, value, windowStart, windowEnd)
        #     VALUES (?, ?, ?, ?)
        #     ''',
        #     (key, value, windowStart, windowEnd)
        # )

        cursor.execute(
            '''
            INSERT INTO top_errors (key, value, windowStart, windowEnd)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(windowStart, windowEnd) DO UPDATE SET
                value = value;
            ''',
            (key, value, windowStart, windowEnd)
        )

        conn.commit()



except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    conn.close()
