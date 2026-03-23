import os
from enum import Enum

class Config(Enum):
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "datavault")
    DB_USER = os.getenv("DB_USER", "dv_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "dv_secret_pass")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events_dv")
