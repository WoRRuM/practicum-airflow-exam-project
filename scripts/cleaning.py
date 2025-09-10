# scripts/cleaning.py
from __future__ import annotations

from typing import Iterable, Optional, Sequence

import numpy as np
import pandas as pd


def remove_feature_duplicates(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """
    Удаляет записи, у которых ВСЕ признаки (кроме id_col) совпадают.
    Удаляем и «оригинал», и «дубликаты» (keep=False), т.к. это, скорее всего, сбой.
    """
    feature_cols: list[str] = df.columns.drop(id_col).tolist()
    dup_mask = df.duplicated(subset=feature_cols, keep=False)
    return df.loc[~dup_mask].reset_index(drop=True)


def fill_missing_values(
    df: pd.DataFrame,
    natural_null_cols: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Числа -> среднее, категориальные -> мода.
    Колонки с естественными пропусками (например, дата окончания сервиса) не трогаем.
    """
    natural_null_cols = set(natural_null_cols or [])
    cols = df.columns[df.isna().any()].difference(natural_null_cols)

    for col in cols:
        if pd.api.types.is_numeric_dtype(df[col]):
            fill_val = float(df[col].mean())
        else:
            mode = df[col].mode(dropna=True)
            fill_val = mode.iloc[0] if not mode.empty else "Unknown"
        df[col] = df[col].fillna(fill_val)
    return df


def remove_outliers_iqr(
    df: pd.DataFrame,
    cols: Iterable[str],
    threshold: float = 1.5,
) -> pd.DataFrame:
    """
    Фильтрует выбросы по IQR для выбранных числовых колонок.
    """
    if not cols:
        return df

    mask = pd.Series(True, index=df.index)
    for c in cols:
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        q1, q3 = df[c].quantile([0.25, 0.75])
        iqr = q3 - q1
        low, high = q1 - threshold * iqr, q3 + threshold * iqr
        mask &= df[c].between(low, high)
    return df.loc[mask].reset_index(drop=True)


def apply_domain_rules_realty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Предметные проверки для данных о квартирах (если таких колонок нет — просто игнорится).
    """
    if {"total_area", "price"}.issubset(df.columns):
        df = df[(df["total_area"] > 0) & (df["price"] > 0)]

    if {"floor", "floors_total"}.issubset(df.columns):
        df = df[df["floor"] <= df["floors_total"]]

    if {"latitude", "longitude"}.issubset(df.columns):
        df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]

    if "ceiling_height" in df.columns:
        df = df[df["ceiling_height"].between(2.2, 4.5, inclusive="both")]

    if "build_year" in df.columns:
        df = df[df["build_year"].between(1800, pd.Timestamp.today().year + 1)]

    # производные без утечки таргета (price НЕ используем)
    if {"kitchen_area", "living_area", "total_area"}.issubset(df.columns):
        with np.errstate(divide="ignore", invalid="ignore"):
            df["kitchen_share"] = (df["kitchen_area"] / df["total_area"]).clip(0, 1)
            df["living_share"] = (df["living_area"] / df["total_area"]).clip(0, 1)
    return df


def basic_clean(
    df: pd.DataFrame,
    id_col: str,
    natural_null_cols: Optional[Sequence[str]] = None,
    iqr_numeric_cols: Optional[Sequence[str]] = None,
    apply_realty_rules: bool = False,
) -> pd.DataFrame:
    """
    Компактный «конвейер» очистки: дубли -> предметные правила -> пропуски -> IQR.
    """
    df = remove_feature_duplicates(df, id_col=id_col)
    if apply_realty_rules:
        df = apply_domain_rules_realty(df)
    df = fill_missing_values(df, natural_null_cols=natural_null_cols)
    if iqr_numeric_cols:
        df = remove_outliers_iqr(df, cols=iqr_numeric_cols)
    return df

