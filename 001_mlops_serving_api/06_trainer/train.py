"""Python Trainer.

Spark が書き出した前処理済み parquet を読み、fraud 判定の二値分類モデルを学習する。
カテゴリ変数は OneHotEncoder、数値はそのまま使い、RandomForest で学習。
学習済みパイプライン(前処理込み)を /models/model.joblib に保存する。
Serving API はこの成果物をロードして予測する。
"""
import json
import os

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

FEATURES_PATH = os.environ.get("FEATURES_PATH", "/data/features/transactions")
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")
META_PATH = os.environ.get("META_PATH", "/models/metadata.json")

CATEGORICAL = ["merchant_category", "country", "device"]
NUMERIC = ["amount", "amount_log", "hour", "dow"]
TARGET = "is_fraud"


def main() -> None:
    df = pd.read_parquet(FEATURES_PATH)
    print(f"[train] loaded {len(df)} rows from {FEATURES_PATH}")

    X = df[CATEGORICAL + NUMERIC]
    y = df[TARGET].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
            ("num", "passthrough", NUMERIC),
        ]
    )
    model = Pipeline(
        steps=[
            ("prep", preprocessor),
            ("clf", RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)),
        ]
    )

    model.fit(X_train, y_train)

    metrics = {"n_train": len(X_train), "n_test": len(X_test)}
    if y_test.nunique() > 1:
        proba = model.predict_proba(X_test)[:, 1]
        metrics["roc_auc"] = round(float(roc_auc_score(y_test, proba)), 4)
        print(f"[train] test ROC-AUC = {metrics['roc_auc']}")
    else:
        print("[train] only one class present in test set; skipping ROC-AUC")

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    metadata = {
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "target": TARGET,
        "metrics": metrics,
    }
    with open(META_PATH, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"[train] saved model -> {MODEL_PATH}")


if __name__ == "__main__":
    main()
