import json
import time
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from config import Config

class PostgresConnection:
    def __init__(self):
        retries = 5
        while retries > 0:
            try:
                self.conn = psycopg2.connect(
                    host=Config.DB_HOST.value,         
                    port=Config.DB_PORT.value,         
                    database=Config.DB_NAME.value,     
                    user=Config.DB_USER.value,         
                    password=Config.DB_PASSWORD.value  
                )
                self.conn.autocommit = True
                self.cursor = self.conn.cursor()
                print("Успешное подключение к PostgreSQL")
                break
            except psycopg2.OperationalError:
                print(f"Ожидание запуска базы данных... (осталось {retries} попыток)")
                time.sleep(3)
                retries -= 1

    def get_cursor(self):
        return self.cursor

    def close(self):
        self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

class KafkaEventProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[Config.KAFKA_BOOTSTRAP_SERVERS.value],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, value):
        self.producer.send(Config.KAFKA_TOPIC.value, value) 

    def close(self):
        self.producer.close()

class KafkaEventConsumer:
    def __init__(self, group_id):
        self.consumer = KafkaConsumer(
            Config.KAFKA_TOPIC.value,
            bootstrap_servers=[Config.KAFKA_BOOTSTRAP_SERVERS.value],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=group_id,
            enable_auto_commit=False, 
            max_poll_records=100    
        )

    def poll(self, timeout_ms=1000):
        return self.consumer.poll(timeout_ms=timeout_ms)

    def commit(self):
        self.consumer.commit()

    def close(self):
        self.consumer.close()
