import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml


def get_connection():
    """Создаёт подключение к БД"""
    load_dotenv()

    host = os.environ.get("DB_DESTINATION_HOST")
    port = os.environ.get("DB_DESTINATION_PORT")
    db = os.environ.get("DB_DESTINATION_NAME")
    username = os.environ.get("DB_DESTINATION_USER")
    password = os.environ.get("DB_DESTINATION_PASSWORD")

    conn = create_engine(
        f"postgresql://{username}:{password}@{host}:{port}/{db}",
        connect_args={"sslmode": "require"},
    )

    return conn


def get_data():
    """Загружает очищенные данные из БД"""
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    conn = get_connection()

    query = "SELECT * FROM clean_real_estate"
    df = pd.read_sql(query, conn, index_col=params["index_col"])

    conn.dispose()

    print(f"Загружено {len(df)} строк, {len(df.columns)} колонок")

    os.makedirs("data", exist_ok=True)
    df.to_csv("data/real_estate_data.csv")

    print("Данные сохранены в data/real_estate_data.csv")


if __name__ == "__main__":
    get_data()
