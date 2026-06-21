"""Scorer(出口の前提).

最新の学習済みモデルをロードし、まだスコアリングしていない取引を ClickHouse から読み、
fraud 確率を計算して dwh.predictions に書き戻す常駐サービス。Grafana はこの結果を
生データ(dwh.transactions)と JOIN して可視化する。

モデルが更新された(再学習された)ことを mtime で検知したら、metadata.json の指標を
dwh.model_runs に記録する(ROC-AUC 推移の可視化用)。
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import joblib
import pandas as pd

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")
META_PATH = os.environ.get("META_PATH", "/models/metadata.json")
SCORE_INTERVAL = float(os.environ.get("SCORE_INTERVAL", "10"))
BATCH = int(os.environ.get("BATCH", "5000"))

FEATURES = ["merchant_category", "country", "device", "amount", "amount_log", "hour", "dow"]

# まだ predictions に無い取引の特徴量を取り出すクエリ。
#   dow は学習時(Spark の dayofweek: 日=1..土=7)に合わせて変換する。
UNSCORED_SQL = f"""
SELECT
    t.tx_uuid                      AS tx_uuid,
    t.amount                       AS amount,
    t.merchant_category            AS merchant_category,
    t.country                      AS country,
    t.device                       AS device,
    toHour(t.created_at)           AS hour,
    (toDayOfWeek(t.created_at) % 7) + 1 AS dow
FROM dwh.transactions AS t FINAL
WHERE t.tx_uuid NOT IN (SELECT tx_uuid FROM dwh.predictions)
LIMIT {BATCH}
"""


def connect():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
    )


def log_model_run(client, model_version: str) -> None:
    """再学習を検知したら metadata.json の指標を model_runs に記録する。"""
    try:
        with open(META_PATH) as f:
            meta = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    m = meta.get("metrics", {})
    # clickhouse-connect は DateTime64 に datetime オブジェクトを要求する(ISO文字列は不可)。
    # tz-aware を UTC naive に変換して渡す。
    raw = meta.get("trained_at")
    try:
        trained_at = datetime.fromisoformat(raw) if raw else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        trained_at = datetime.now(timezone.utc)
    if trained_at.tzinfo is not None:
        trained_at = trained_at.astimezone(timezone.utc).replace(tzinfo=None)
    client.insert(
        "dwh.model_runs",
        [[model_version, trained_at, int(m.get("n_train") or 0),
          int(m.get("n_test") or 0), float(m.get("roc_auc") or 0.0)]],
        column_names=["model_version", "trained_at", "n_train", "n_test", "roc_auc"],
    )
    print(f"[scorer] logged model_run version={model_version} roc_auc={m.get('roc_auc')}")


def score_once(client, model, model_version: str) -> int:
    df = client.query_df(UNSCORED_SQL)
    if df.empty:
        return 0
    df["amount_log"] = df["amount"].map(lambda a: math.log1p(a))
    proba = model.predict_proba(df[FEATURES])[:, 1]

    rows = [
        [tx, float(p), int(p >= 0.5), model_version]
        for tx, p in zip(df["tx_uuid"], proba)
    ]
    client.insert(
        "dwh.predictions", rows,
        column_names=["tx_uuid", "fraud_probability", "pred_label", "model_version"],
    )
    return len(rows)


def main() -> None:
    print(f"[scorer] start: host={CLICKHOUSE_HOST} interval={SCORE_INTERVAL}s model={MODEL_PATH}")
    client = connect()
    model = None
    loaded_mtime = None

    while True:
        try:
            if os.path.exists(MODEL_PATH):
                mtime = os.path.getmtime(MODEL_PATH)
                if mtime != loaded_mtime:
                    model = joblib.load(MODEL_PATH)
                    loaded_mtime = mtime
                    version = str(int(mtime))
                    print(f"[scorer] loaded model version={version}")
                    log_model_run(client, version)
                version = str(int(loaded_mtime))
                n = score_once(client, model, version)
                if n:
                    print(f"[scorer] scored {n} rows (version={version})")
            else:
                print("[scorer] model not found yet; run the trainer (make train)")
        except Exception as exc:  # noqa: BLE001
            print(f"[scorer] error: {exc}")
            try:
                client = connect()
            except Exception:  # noqa: BLE001
                pass
        time.sleep(SCORE_INTERVAL)


if __name__ == "__main__":
    main()
