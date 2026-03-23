from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.sensors.postgres import PostgresSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='stage_to_datavault_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *', # Запуск каждые 5 минут
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. Сенсор: Проверяем, есть ли новые (необработанные) данные в Stage
    check_new_data = PostgresSensor(
        task_id='check_new_data_in_stage',
        postgres_conn_id='postgres_dwh',
        sql="SELECT 1 FROM stg_events WHERE is_processed = FALSE LIMIT 1;",
        mode='poke',
        timeout=300,
        poke_interval=30
    )

    # 2. Загрузка HUBs (Хабы грузятся первыми!)
    load_hubs = PostgresOperator(
        task_id='load_hubs',
        postgres_conn_id='postgres_dwh',
        sql="""
        -- Hub User (извлекаем ID юзера из ЛЮБОГО события, где он есть)
        INSERT INTO hub_user (user_hk, user_id, load_dts, rec_src)
        SELECT DISTINCT md5(payload->>'user_id'), (payload->>'user_id')::INT, CURRENT_TIMESTAMP, 'airflow_etl'
        FROM stg_events WHERE is_processed = FALSE AND payload ? 'user_id'
        ON CONFLICT (user_hk) DO NOTHING;

        -- Hub Platform
        INSERT INTO hub_platform (platform_hk, platform_name, load_dts, rec_src)
        SELECT DISTINCT md5(payload->>'platform'), payload->>'platform', CURRENT_TIMESTAMP, 'airflow_etl'
        FROM stg_events WHERE is_processed = FALSE AND payload ? 'platform'
        ON CONFLICT (platform_hk) DO NOTHING;

        -- Hub Content
        INSERT INTO hub_content (content_hk, content_id, load_dts, rec_src)
        SELECT DISTINCT md5(payload->>'content_id'), (payload->>'content_id')::INT, CURRENT_TIMESTAMP, 'airflow_etl'
        FROM stg_events WHERE is_processed = FALSE AND payload ? 'content_id'
        ON CONFLICT (content_hk) DO NOTHING;
        """
    )

    # 3. Загрузка LINKS (Теперь мы уверены, что Хабы существуют)
    load_links = PostgresOperator(
        task_id='load_links',
        postgres_conn_id='postgres_dwh',
        sql="""
        -- Link User Login
        INSERT INTO t_link_user_login (login_hk, user_hk, platform_hk, event_dts, load_dts, rec_src)
        SELECT DISTINCT md5(payload->>'event_id'), md5(payload->>'user_id'), md5(payload->>'platform'), 
               (payload->>'timestamp')::TIMESTAMP, CURRENT_TIMESTAMP, 'airflow_etl'
        FROM stg_events WHERE is_processed = FALSE AND event_type = 'LOGIN'
        ON CONFLICT (login_hk) DO NOTHING;

        -- Link User Action
        INSERT INTO t_link_user_action (action_hk, user_hk, content_hk, action_type, action_value, event_dts, load_dts, rec_src)
        SELECT DISTINCT md5(payload->>'event_id'), md5(payload->>'user_id'), md5(payload->>'content_id'), 
               payload->>'action_type', payload->>'action_value', (payload->>'timestamp')::TIMESTAMP, CURRENT_TIMESTAMP, 'airflow_etl'
        FROM stg_events WHERE is_processed = FALSE AND event_type = 'ACTION'
        ON CONFLICT (action_hk) DO NOTHING;
        """
    )

    # 4. Загрузка SATELLITES (Вставка новой истории и закрытие старой)
    # Для простоты примера реализован insert on conflict (в проде нужен полноценный SCD2 merge)
    load_satellites = PostgresOperator(
        task_id='load_satellites',
        postgres_conn_id='postgres_dwh',
        sql="""
        -- Sat User Profile
        INSERT INTO sat_user_profile (user_hk, hash_diff, subscription_level, city, valid_from, valid_to, is_active)
        SELECT md5(payload->>'user_id'), md5(CONCAT(payload->>'subscription_level', payload->>'city')), 
               payload->>'subscription_level', payload->>'city', (payload->>'timestamp')::TIMESTAMP, '9999-12-31', TRUE
        FROM stg_events WHERE is_processed = FALSE AND event_type = 'PROFILE_UPDATE'
        ON CONFLICT (user_hk, hash_diff) DO NOTHING;
        """
    )

    # 5. Помечаем данные как обработанные
    mark_processed = PostgresOperator(
        task_id='mark_processed',
        postgres_conn_id='postgres_dwh',
        sql="UPDATE stg_events SET is_processed = TRUE WHERE is_processed = FALSE;"
    )

    # Выстраиваем граф зависимостей (ETL Pipeline)
    check_new_data >> load_hubs >> load_links >> load_satellites >> mark_processed
