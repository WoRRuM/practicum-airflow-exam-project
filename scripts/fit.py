import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import yaml
import os
import joblib


def fit_model():
    """Обучает модель регрессии"""
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    target_col = params["target_col"]
    test_size = params["test_size"]
    random_state = params["random_state"]
    model_params = params["model_params"]

    print(f"Параметры модели: {model_params}")

    df = pd.read_csv("data/real_estate_data.csv", index_col="id")
    print(f"Загружено {len(df)} строк")

    X = df.drop(columns=[target_col])
    y = df[target_col]

    # Обрабатываем категориальные признаки (boolean)
    bool_cols = X.select_dtypes(include=["bool"]).columns
    X[bool_cols] = X[bool_cols].astype(int)

    print(f"Признаков: {X.shape[1]}, объектов: {len(X)}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    pipeline = Pipeline(
        [("scaler", StandardScaler()), ("model", RandomForestRegressor(**model_params))]
    )

    print("Обучение модели...")
    pipeline.fit(X_train, y_train)

    train_score = pipeline.score(X_train, y_train)
    test_score = pipeline.score(X_test, y_test)

    print(f"Train R²: {train_score:.4f}")
    print(f"Test R²: {test_score:.4f}")

    os.makedirs("models", exist_ok=True)

    with open("models/fitted_model.pkl", "wb") as fd:
        joblib.dump(pipeline, fd)

    print("Модель сохранена в models/fitted_model.pkl")


if __name__ == "__main__":
    fit_model()
