import json
import time
from connction import PostgresConnection, KafkaEventProducer, KafkaEventConsumer
from generators import EventGenerator

def run_producer():
 
    producer = KafkaEventProducer()
    generator = EventGenerator()

    print("Старт генерации событий")
    while True:
        event = generator.get_random_event()
        producer.send(event) 
        print(f"Отправлено: {event['event_type']}")
        time.sleep(2)

def run_consumer():
    db = PostgresConnection()
    cursor = db.get_cursor()


    consumer = KafkaEventConsumer(group_id='stage_loader_group')

    print("Старт загрузки в STAGE слой")
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
            print(f"Записано {len(messages)} событий в STAGE.")

if __name__ == "__main__":
    time.sleep(10)
    threading.Thread(target=run_producer, daemon=True).start()
    run_consumer()
