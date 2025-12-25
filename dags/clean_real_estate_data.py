from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="clean_real_estate_data",
    default_args=default_args,
    description="Очищает данные от дубликатов, пропусков и выбросов",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["real_estate", "data_cleaning"],
)
def clean_real_estate_pipeline():
    @task()
    def create_table():
        """Создаёт таблицу для очищенных данных"""
        hook = PostgresHook(postgres_conn_id="destination_db")

        create_query = """
        DROP TABLE IF EXISTS clean_real_estate CASCADE;
        
        CREATE TABLE clean_real_estate (
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
        return "Clean table created"

    @task()
    def extract():
        """Извлекает сырые данные"""
        hook = PostgresHook(postgres_conn_id="destination_db")
        conn = hook.get_conn()

        df = pd.read_sql("SELECT * FROM raw_real_estate", conn)
        conn.close()

        print(f"Извлечено {len(df)} строк")
        return df.to_json()

    @task()
    def remove_duplicates(data_json: str):
        """Удаляет дубликаты"""
        df = pd.read_json(data_json)
        initial_len = len(df)

        df = df.drop_duplicates()

        removed = initial_len - len(df)
        print(f"Удалено дубликатов: {removed}")

        return df.to_json()

    @task()
    def handle_missing_values(data_json: str):
        """Обрабатывает пропущенные значения"""
        df = pd.read_json(data_json)
        initial_len = len(df)

        critical_cols = ["price", "total_area", "rooms"]
        df = df.dropna(subset=critical_cols)

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                df[col].fillna(df[col].median(), inplace=True)

        removed = initial_len - len(df)
        print(f"Удалено строк с пропусками: {removed}")

        return df.to_json()

    @task()
    def remove_outliers(data_json: str):
        """Удаляет выбросы методом IQR"""
        df = pd.read_json(data_json)
        initial_len = len(df)

        for col in ["price", "total_area"]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            df = df[(df[col] >= lower) & (df[col] <= upper)]

        removed = initial_len - len(df)
        print(f"Удалено выбросов: {removed}")

        return df.to_json()

    @task()
    def load(data_json: str):
        """Загружает очищенные данные"""
        df = pd.read_json(data_json)
        hook = PostgresHook(postgres_conn_id="destination_db")

        # Удаляем колонку id перед вставкой
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        hook.insert_rows(
            table="clean_real_estate",
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
            commit_every=1000,
        )

        print(f"Загружено {len(df)} строк в clean_real_estate")
        return f"Loaded {len(df)} rows"

    @task(trigger_rule="all_success")
    def send_telegram_success():
        """Отправляет уведомление об успехе"""
        from plugins.telegram_plugin import send_telegram_message

        message = "✅ DAG clean_real_estate_data успешно завершён!"
        send_telegram_message(message)
        return "Success notification sent"

    @task(trigger_rule="one_failed")
    def send_telegram_failure():
        """Отправляет уведомление об ошибке"""
        from plugins.telegram_plugin import send_telegram_message

        message = "❌ DAG clean_real_estate_data завершился с ошибкой!"
        send_telegram_message(message)
        return "Failure notification sent"

    table_created = create_table()
    extracted = extract()
    no_duplicates = remove_duplicates(extracted)
    no_missing = handle_missing_values(no_duplicates)
    no_outliers = remove_outliers(no_missing)
    loaded = load(no_outliers)

    success = send_telegram_success()
    failure = send_telegram_failure()

    (
        table_created
        >> extracted
        >> no_duplicates
        >> no_missing
        >> no_outliers
        >> loaded
        >> [success, failure]
    )


dag = clean_real_estate_pipeline()
