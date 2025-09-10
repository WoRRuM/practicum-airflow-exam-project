# dags/flats_clean_dataset.py
from __future__ import annotations

import pendulum
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.cleaning import basic_clean

# from steps.messages import send_telegram_success_message, send_telegram_failure_message


@dag(
    dag_id="flats_clean_dataset",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["realty", "clean"],
    # on_success_callback=send_telegram_success_message,
    # on_failure_callback=send_telegram_failure_message,
)
def flats_clean_dataset():
    """
    Чистим flats_dataset_raw и сохраняем в flats_dataset_clean.
    """

    @task()
    def create_table() -> None:
        from sqlalchemy import (
            Boolean,
            Column,
            Float,
            Integer,
            MetaData,
            String,
            Table,
            DateTime,
            UniqueConstraint,
            inspect,
        )

        hook = PostgresHook("destination_db")
        engine = hook.get_sqlalchemy_engine()
        md = MetaData()

        table = Table(
            "flats_dataset_clean",
            md,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("flat_id", Integer, nullable=False),
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
            Column("kitchen_share", Float),
            Column("living_share", Float),
            Column("ingested_at", DateTime),
            UniqueConstraint("flat_id", name="uq_clean_flat_id"),
        )

        if not inspect(engine).has_table(table.name):
            md.create_all(engine)

    @task()
    def extract() -> pd.DataFrame:
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        df = pd.read_sql("SELECT * FROM flats_dataset_raw", conn)
        conn.close()
        return df

    @task()
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        # основная очистка и предметные правила для недвижимости
        num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
        df = basic_clean(
            df,
            id_col="flat_id",
            natural_null_cols=[],  # для realty естественных NaN нет
            iqr_numeric_cols=[c for c in num_cols if c not in {"price"}],
            apply_realty_rules=True,
        )

        # финальный доводчик типов
        bool_like = ["is_apartment", "studio", "has_elevator"]
        for c in bool_like:
            if c in df.columns:
                df[c] = df[c].astype(bool)

        # даты: оставим как есть (ingested_at уже датавремя)
        return df

    @task()
    def load(df: pd.DataFrame) -> None:
        hook = PostgresHook("destination_db")
        df.to_sql(
            name="flats_dataset_clean",
            con=hook.get_sqlalchemy_engine(),
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000,
        )

    create_table()
    cleaned = transform(extract())
    load(cleaned)


flats_clean_dataset()

