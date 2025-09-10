# scripts/fit.py
from __future__ import annotations

import os

import joblib
import pandas as pd
import yaml
from category_encoders import CatBoostEncoder
from catboost import CatBoostClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from scripts.cleaning import basic_clean


def fit_model():
    # 1) гиперпараметры
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    index_col = params["index_col"]
    target_col = params["target_col"]
    one_hot_drop = params["one_hot_drop"]
    auto_class_weights = params["auto_class_weights"]

    # 2) загрузка предыдущего шага
    df = pd.read_csv("data/initial_data.csv")

    # 3) очистка (строго до построения пайплайна)
    natural_nulls = params.get("natural_null_cols", ["end_date"]) if "end_date" in df.columns else []
    num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
    df = basic_clean(
        df,
        id_col=index_col,
        natural_null_cols=natural_nulls,
        iqr_numeric_cols=[c for c in num_cols if c != target_col],
        apply_realty_rules=False,
    )

    # если вдруг остались пропуски в таргете — удалим такие строки
    df = df.dropna(subset=[target_col])

    # 4) признаки
    cat = df.select_dtypes(include=["object"]).columns.tolist()
    potential_binary = df[cat].nunique() == 2 if cat else pd.Series(dtype=bool)
    binary_cat = list(potential_binary[potential_binary].index)
    other_cat = list(potential_binary[~potential_binary].index)
    num = [c for c in df.select_dtypes(include=["float", "int"]).columns if c != target_col]

    preprocessor = ColumnTransformer(
        transformers=[
            ("binary", OneHotEncoder(drop=one_hot_drop, handle_unknown="ignore"), binary_cat),
            ("cat", CatBoostEncoder(return_df=False), other_cat),
            ("num", StandardScaler(), num),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = CatBoostClassifier(
        auto_class_weights=auto_class_weights,
        verbose=False,
        random_seed=42,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    pipeline.fit(df, df[target_col])

    os.makedirs("models", exist_ok=True)
    with open("models/fitted_model.pkl", "wb") as fd:
        joblib.dump(pipeline, fd)


if __name__ == "__main__":
    fit_model()

