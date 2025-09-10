# dags/flats_build_dataset.py
from __future__ import annotations

import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# если используешь callbacks телеграма:
# from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    dag_id="flats_build_dataset",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["realty", "build"],
    # on_success_callback=send_telegram_success_message,
    # on_failure_callback=send_telegram_failure_message,
)
def flats_build_dataset():
    """
    Собираем датасет квартир = flats ⨉ buildings, сохраняем в таблицу flats_dataset_raw.
    """

    @task()
    def create_table() -> None:
        from sqlalchemy import (
            Column,
            Float,
            Integer,
            MetaData,
            String,
            Table,
            Boolean,
            DateTime,
            inspect,
        )

        hook = PostgresHook("destination_db")
        engine = hook.get_sqlalchemy_engine()
        md = MetaData()

        table = Table(
            "flats_dataset_raw",
            md,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("flat_id", Integer, nullable=False, unique=True),
            Column("building_id", Integer, nullable=False),
            Column("price", Float),
            Column("total_area", Float),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("is_apartment", Boolean),
            Column("studio", Boolean),
            Column("floor", Integer),
            Column("floors_total", Integer),
            Column("build_year", Integer),
            Column("building_type_int", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("has_elevator", Boolean),
            Column("ingested_at", DateTime),
        )

        if not inspect(engine).has_table(table.name):
            md.create_all(engine)

    @task()
    def extract() -> pd.DataFrame:
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        sql = """
        SELECT
            f.id               AS flat_id,
            f.building_id,
            f.price,
            f.total_area,
            f.kitchen_area,
            f.living_area,
            f.rooms,
            f.is_apartment,
            f.studio,
            f.floor,
            b.floors_total,
            b.build_year,
            b.building_type_int,
            b.latitude,
            b.longitude,
            b.ceiling_height,
            b.flats_count,
            b.has_elevator,
            NOW()              AS ingested_at
        FROM flats AS f
        JOIN buildings AS b
          ON b.id = f.building_id;
        """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df

    @task()
    def load(df: pd.DataFrame) -> None:
        hook = PostgresHook("destination_db")
        df.to_sql(
            name="flats_dataset_raw",
            con=hook.get_sqlalchemy_engine(),
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000,
        )

    create_table()
    load(extract())


flats_build_dataset()

