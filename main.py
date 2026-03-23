import json
import time
import threading
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from config import Config
from generators import EventGenerator

def get_db_connection():
    return psycopg2.connect(
        host=Config.DB_HOST.value, port=Config.DB_PORT.value,
        database=Config.DB_NAME.value, user=Config.DB_USER.value, password=Config.DB_PASSWORD.value
    )

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[Config.KAFKA_BOOTSTRAP_SERVERS.value],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    generator = EventGenerator()
    print("[Producer] Старт генерации событий...")
    while True:
        event = generator.get_random_event()
        producer.send(Config.KAFKA_TOPIC.value, event)
        print(f"[Producer] Отправлено: {event['event_type']}")
        time.sleep(2)

def run_consumer():
    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC.value,
        bootstrap_servers=[Config.KAFKA_BOOTSTRAP_SERVERS.value],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='stage_loader_group',
        enable_auto_commit=False,
        max_poll_records=100
    )

    print("[Consumer] Старт загрузки в STAGE слой...")
    while True:
        msg_batches = consumer.poll(timeout_ms=1000)
        if not msg_batches:
            continue

        for tp, messages in msg_batches.items():
            for message in messages:
                event = message.value
                cursor.execute("""
                    INSERT INTO stg_events (event_type, payload, event_dts)
                    VALUES (%s, %s, %s)
                """, (event['event_type'], json.dumps(event), event['timestamp']))

            consumer.commit()
            print(f"[Consumer] Записано {len(messages)} событий в STAGE.")

if __name__ == "__main__":
    time.sleep(10)
    threading.Thread(target=run_producer, daemon=True).start()
    run_consumer()
