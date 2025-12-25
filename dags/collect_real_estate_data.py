from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="collect_real_estate_data",
    default_args=default_args,
    description="Собирает данные из таблиц buildings и flats",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["real_estate", "data_collection"],
)
def collect_real_estate_pipeline():
    @task()
    def create_table():
        """Создаёт таблицу для объединённого датасета"""
        hook = PostgresHook(postgres_conn_id="destination_db")

        create_query = """
        DROP TABLE IF EXISTS raw_real_estate CASCADE;
        
        CREATE TABLE raw_real_estate (
            id SERIAL PRIMARY KEY,
            building_id INTEGER,
            floor INTEGER,
            kitchen_area FLOAT,
            living_area FLOAT,
            rooms INTEGER,
            is_apartment BOOLEAN,
            studio BOOLEAN,
            total_area FLOAT,
            price FLOAT,
            build_year INTEGER,
            building_type_int INTEGER,
            latitude FLOAT,
            longitude FLOAT,
            ceiling_height FLOAT,
            flats_count INTEGER,
            floors_total INTEGER,
            has_elevator BOOLEAN
        );
        """

        hook.run(create_query)
        return "Table created"

    @task()
    def extract():
        """Извлекает данные из таблиц buildings и flats"""
        hook = PostgresHook(postgres_conn_id="destination_db")
        conn = hook.get_conn()

        query = """
        SELECT 
            f.id,
            f.building_id,
            f.floor,
            f.kitchen_area,
            f.living_area,
            f.rooms,
            f.is_apartment,
            f.studio,
            f.total_area,
            f.price,
            b.build_year,
            b.building_type_int,
            b.latitude,
            b.longitude,
            b.ceiling_height,
            b.flats_count,
            b.floors_total,
            b.has_elevator
        FROM flats f
        LEFT JOIN buildings b ON f.building_id = b.id
        """

        df = pd.read_sql(query, conn)
        conn.close()

        return df.to_json()

    @task()
    def transform(data_json: str):
        """Базовая трансформация данных"""
        df = pd.read_json(data_json)

        print(f"Загружено строк: {len(df)}")
        print(f"Колонок: {len(df.columns)}")

        return df.to_json()

    @task()
    def load(data_json: str):
        """Загружает данные в таблицу raw_real_estate"""
        df = pd.read_json(data_json)
        hook = PostgresHook(postgres_conn_id="destination_db")

        hook.insert_rows(
            table="raw_real_estate",
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
            commit_every=1000,
        )

        print(f"Загружено {len(df)} строк в raw_real_estate")
        return f"Loaded {len(df)} rows"

    @task(trigger_rule="all_success")
    def send_telegram_success():
        """Отправляет уведомление об успехе в Telegram"""
        from plugins.telegram_plugin import send_telegram_message

        message = "✅ DAG collect_real_estate_data успешно завершён!"
        send_telegram_message(message)
        return "Telegram notification sent"

    @task(trigger_rule="one_failed")
    def send_telegram_failure():
        """Отправляет уведомление об ошибке в Telegram"""
        from plugins.telegram_plugin import send_telegram_message

        message = "❌ DAG collect_real_estate_data завершился с ошибкой!"
        send_telegram_message(message)
        return "Telegram failure notification sent"

    table_created = create_table()
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    loaded = load(transformed_data)

    success = send_telegram_success()
    failure = send_telegram_failure()

    table_created >> extracted_data >> transformed_data >> loaded >> [success, failure]


dag = collect_real_estate_pipeline()
