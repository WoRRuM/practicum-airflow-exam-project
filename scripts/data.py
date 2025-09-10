# scripts/data.py
from __future__ import annotations

import os

import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine

from scripts.cleaning import basic_clean


def create_connection():
    load_dotenv()
    host = os.environ.get("DB_DESTINATION_HOST")
    port = os.environ.get("DB_DESTINATION_PORT")
    db = os.environ.get("DB_DESTINATION_NAME")
    user = os.environ.get("DB_DESTINATION_USER")
    pwd = os.environ.get("DB_DESTINATION_PASSWORD")
    # sslmode=require — как в учебной БД
    return create_engine(
        f"postgresql://{user}:{pwd}@{host}:{port}/{db}",
        connect_args={"sslmode": "require"},
    )


def get_data():
    # 1) гиперпараметры
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    index_col = params["index_col"]
    # необязательный параметр — откуда брать «сырой» датасет
    source_table = params.get("source_table", "clean_users_churn")

    # 2) загрузка
    eng = create_connection()
    df = pd.read_sql(f"select * from {source_table}", eng)
    eng.dispose()

    # 3) безопасная первичная очистка (без утечек)
    # для churn: естественные пропуски бывают в 'end_date'
    natural_nulls = params.get("natural_null_cols", ["end_date"]) if "end_date" in df.columns else []
    num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
    df = basic_clean(
        df,
        id_col=index_col,
        natural_null_cols=natural_nulls,
        iqr_numeric_cols=[c for c in num_cols if c != params.get("target_col", "target")],
        apply_realty_rules=False,  # для churn-курса
    )

    # 4) сохранение
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/initial_data.csv", index=False)


if __name__ == "__main__":
    get_data()

