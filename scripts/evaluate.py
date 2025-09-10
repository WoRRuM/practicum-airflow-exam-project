# scripts/evaluate.py
from __future__ import annotations

import json
import os

import joblib
import pandas as pd
import yaml
from sklearn.model_selection import StratifiedKFold, cross_validate

from scripts.cleaning import basic_clean


def evaluate_model():
    # 1) гиперпараметры
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    index_col = params["index_col"]
    target_col = params["target_col"]
    n_splits = int(params["n_splits"])
    n_jobs = int(params["n_jobs"])
    metrics = list(params["metrics"])

    # 2) данные и модель
    df = pd.read_csv("data/initial_data.csv")
    with open("models/fitted_model.pkl", "rb") as fd:
        pipeline = joblib.load(fd)

    # 3) совпадающая очистка
    natural_nulls = params.get("natural_null_cols", ["end_date"]) if "end_date" in df.columns else []
    num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
    df = basic_clean(
        df,
        id_col=index_col,
        natural_null_cols=natural_nulls,
        iqr_numeric_cols=[c for c in num_cols if c != target_col],
        apply_realty_rules=False,
    )
    df = df.dropna(subset=[target_col])

    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    raw = cross_validate(
        pipeline,
        X=df,
        y=df[target_col],
        scoring=metrics,
        cv=cv,
        n_jobs=n_jobs,
        return_train_score=False,
    )

    cv_res = {k: round(float(pd.Series(v).mean()), 3) for k, v in raw.items() if hasattr(v, "__len__")}

    os.makedirs("cv_results", exist_ok=True)
    with open("cv_results/cv_res.json", "w") as fd:
        json.dump(cv_res, fd, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    evaluate_model()

