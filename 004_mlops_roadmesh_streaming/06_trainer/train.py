"""Python Trainer(渋滞レベル分類).

前処理済み parquet を読み、区間の渋滞レベル(0=smooth / 1=slow / 2=congested)を
予測する多クラス分類モデルを学習して /models に保存する。
保存した model.joblib を stream-scorer がロードしてリアルタイムに渋滞を予測する。
"""
import json
import os
from datetime import datetime, timezone

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

FEATURES_PATH = os.environ.get("FEATURES_PATH", "/data/features/links")
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")
META_PATH = os.environ.get("META_PATH", "/models/metadata.json")

CATEGORICAL = ["road_type"]
NUMERIC = ["maxspeed", "sim_hour", "sim_dow"]
TARGET = "congestion"   # 0/1/2


def main() -> None:
    df = pd.read_parquet(FEATURES_PATH)
    print(f"[train] loaded {len(df)} rows from {FEATURES_PATH}")

    X = df[CATEGORICAL + NUMERIC]
    y = df[TARGET].astype(int)

    stratify = y if y.nunique() > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=stratify
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

    acc = float(accuracy_score(y_test, model.predict(X_test))) if len(X_test) else 0.0
    metrics = {"n_train": len(X_train), "n_test": len(X_test), "accuracy": round(acc, 4)}
    print(f"[train] test accuracy = {metrics['accuracy']}")

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    metadata = {
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "target": TARGET,
        "classes": [0, 1, 2],
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
    }
    with open(META_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"[train] saved model -> {MODEL_PATH}")


if __name__ == "__main__":
    main()
