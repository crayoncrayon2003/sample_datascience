"""Python Trainer(MLflow Tracking + Model Registry 版).

004 までは学習結果を `model.joblib` + `metadata.json` としてボリュームに dump していた。
本サンプルでは:

  1. 学習した sklearn Pipeline を **MLflow Tracking** に run として記録(params / metrics)
  2. その run のモデルを Model Registry に `fraud` という名前で **version 登録**
  3. 登録した version に **alias `challenger`** を付与(= 次の昇格候補)

実際に推論へ出る version は別途 `promote.py`(昇格ゲート)が alias `champion` を
付け替えて決める。Scorer は `models:/fraud@champion` を解決して読み込む。

> NOTE: MLflow の旧 "stage"(Staging/Production)は 2.9 以降 **非推奨**。
>       本サンプルは後継の **alias**(champion / challenger)で世代を管理する。
"""
import os

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

FEATURES_PATH = os.environ.get("FEATURES_PATH", "/data/features/transactions")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "fraud")
CHALLENGER_ALIAS = os.environ.get("CHALLENGER_ALIAS", "challenger")

CATEGORICAL = ["merchant_category", "country", "device"]
NUMERIC = ["amount", "amount_log", "hour", "dow"]
TARGET = "is_fraud"

N_ESTIMATORS = int(os.environ.get("N_ESTIMATORS", "200"))


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)

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
            ("clf", RandomForestClassifier(n_estimators=N_ESTIMATORS, random_state=42, n_jobs=-1)),
        ]
    )
    model.fit(X_train, y_train)

    roc_auc = 0.0
    if y_test.nunique() > 1:
        proba = model.predict_proba(X_test)[:, 1]
        roc_auc = round(float(roc_auc_score(y_test, proba)), 4)
        print(f"[train] test ROC-AUC = {roc_auc}")

    # --- MLflow に run として記録し、モデルを Registry に version 登録する --------
    with mlflow.start_run() as run:
        mlflow.log_params(
            {"n_estimators": N_ESTIMATORS, "categorical": CATEGORICAL, "numeric": NUMERIC}
        )
        mlflow.log_metrics(
            {"roc_auc": roc_auc, "n_train": len(X_train), "n_test": len(X_test)}
        )
        signature = infer_signature(X_train, model.predict(X_train))
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME,
        )
        run_id = run.info.run_id
    print(f"[train] logged run {run_id} (roc_auc={roc_auc})")

    # 今 run で登録された version を特定し、challenger alias を付け替える。
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    versions = client.search_model_versions(f"run_id='{run_id}'")
    # alias API は version を文字列で要求する(int を渡すと TypeError)。
    version = str(max(int(v.version) for v in versions))
    client.set_registered_model_alias(MODEL_NAME, CHALLENGER_ALIAS, version)
    print(f"[train] registered {MODEL_NAME} v{version} -> @{CHALLENGER_ALIAS}")


if __name__ == "__main__":
    main()
