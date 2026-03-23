FROM apache/airflow:2.10.4-python3.12

USER root

# Устанавливаем необходимые системные библиотеки
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Переключаемся обратно на пользователя airflow для установки python-пакетов
USER airflow

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
