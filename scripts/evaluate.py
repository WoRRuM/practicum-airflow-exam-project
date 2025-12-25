import pandas as pd
from sklearn.model_selection import cross_validate, KFold
import joblib
import json
import yaml
import os
import numpy as np


def evaluate_model():
    """Оценивает качество модели через кросс-валидацию"""
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    target_col = params["target_col"]
    cv_params = params["cv_params"]

    print(f"Параметры CV: {cv_params}")

    df = pd.read_csv("data/real_estate_data.csv", index_col="id")
    print(f"Загружено {len(df)} строк")

    with open("models/fitted_model.pkl", "rb") as fd:
        pipeline = joblib.load(fd)

    print("Модель загружена")

    X = df.drop(columns=[target_col])
    y = df[target_col]

    bool_cols = X.select_dtypes(include=["bool"]).columns
    X[bool_cols] = X[bool_cols].astype(int)

    cv_strategy = KFold(n_splits=cv_params["n_splits"], shuffle=True, random_state=42)

    print("Запуск кросс-валидации...")

    cv_results = cross_validate(
        estimator=pipeline,
        X=X,
        y=y,
        cv=cv_strategy,
        n_jobs=cv_params["n_jobs"],
        scoring=cv_params["metrics"],
        return_train_score=True,
    )

    formatted_results = {}
    for key, values in cv_results.items():
        formatted_results[key] = round(float(np.mean(values)), 4)

    print("\nРезультаты кросс-валидации:")
    for key, value in formatted_results.items():
        print(f"  {key}: {value}")

    # Добавляем RMSE и MAE (переводим из negative)
    if "test_neg_mean_squared_error" in formatted_results:
        rmse = np.sqrt(-formatted_results["test_neg_mean_squared_error"])
        formatted_results["test_rmse"] = round(rmse, 4)
        print(f"  test_rmse: {formatted_results['test_rmse']}")

    if "test_neg_mean_absolute_error" in formatted_results:
        mae = -formatted_results["test_neg_mean_absolute_error"]
        formatted_results["test_mae"] = round(mae, 4)
        print(f"  test_mae: {formatted_results['test_mae']}")

    os.makedirs("metrics", exist_ok=True)

    with open("metrics/cv_results.json", "w") as fd:
        json.dump(formatted_results, fd, indent=2)

    print("\nРезультаты сохранены в metrics/cv_results.json")


if __name__ == "__main__":
    evaluate_model()
