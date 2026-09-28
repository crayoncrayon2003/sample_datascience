"""ドリフト/データ品質モニタ(常駐).

学習時の特徴量分布(= モデルが見た世界)を **基準(reference)** とし、本番に流れてくる
直近データ(current)と比べて「分布がどれだけズレたか(ドリフト)」を定期的に測る。
計算には **Evidently** を使い、結果を ClickHouse(dwh.drift_metrics / dwh.drift_by_feature)へ
書き戻す。Grafana はそれを時系列で可視化する。

  - reference : Spark 前処理が出した学習用 parquet(/data/features/transactions)
  - current   : dwh.transactions の直近ウィンドウ(MONITOR_WINDOW 件)
  - 監視特徴量: amount(数値)/ merchant_category / country / device(カテゴリ)
  - 併せて予測の要約(直近の fraud 率・平均確率)とデータ品質(欠損率)も記録する。

env: CLICKHOUSE_HOST / CLICKHOUSE_PORT / REF_PATH / MONITOR_INTERVAL / MONITOR_WINDOW / MIN_ROWS
"""
from __future__ import annotations

import os
import time

import clickhouse_connect
import pandas as pd
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
REF_PATH = os.environ.get("REF_PATH", "/data/features/transactions")
INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "30"))
WINDOW = int(os.environ.get("MONITOR_WINDOW", "2000"))
MIN_ROWS = int(os.environ.get("MIN_ROWS", "200"))

NUMERIC = ["amount"]
CATEGORICAL = ["merchant_category", "country", "device"]
FEATURES = NUMERIC + CATEGORICAL
COLUMN_MAPPING = ColumnMapping(numerical_features=NUMERIC, categorical_features=CATEGORICAL)


def ch():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
    )


def load_reference() -> pd.DataFrame | None:
    """学習用 parquet を基準として読む。まだ無ければ None(モデル未学習)。"""
    try:
        df = pd.read_parquet(REF_PATH)
    except (FileNotFoundError, OSError, ValueError):
        return None
    if df.empty:
        return None
    return df


def current_window(client) -> pd.DataFrame:
    return client.query_df(
        f"SELECT amount, merchant_category, country, device "
        f"FROM dwh.transactions ORDER BY created_at DESC LIMIT {WINDOW}"
    )


def prediction_summary(client):
    df = client.query_df(
        f"SELECT fraud_probability, pred_label "
        f"FROM dwh.predictions ORDER BY scored_at DESC LIMIT {WINDOW}"
    )
    if df.empty:
        return 0.0, 0.0
    return float(df["pred_label"].mean()), float(df["fraud_probability"].mean())


def compute_drift(reference: pd.DataFrame, current: pd.DataFrame):
    """Evidently で特徴量ドリフトを計算し、(全体サマリ, 列ごと) を返す。"""
    report = Report(metrics=[DataDriftPreset()])
    report.run(
        reference_data=reference[FEATURES],
        current_data=current[FEATURES],
        column_mapping=COLUMN_MAPPING,
    )
    res = report.as_dict()
    metrics = {m["metric"]: m["result"] for m in res["metrics"]}
    table = metrics.get("DataDriftTable", {})
    summary = {
        "n_features": int(table.get("number_of_columns", len(FEATURES))),
        "n_drifted": int(table.get("number_of_drifted_columns", 0)),
        "drift_share": float(table.get("share_of_drifted_columns", 0.0)),
        "dataset_drift": 1 if table.get("dataset_drift") else 0,
    }
    by_feature = []
    for col, info in (table.get("drift_by_columns") or {}).items():
        by_feature.append(
            [str(col), float(info.get("drift_score") or 0.0), 1 if info.get("drift_detected") else 0]
        )
    return summary, by_feature


def main() -> None:
    print(f"[monitor] start: interval={INTERVAL}s window={WINDOW} ref={REF_PATH}", flush=True)
    client = ch()

    while True:
        reference = load_reference()
        if reference is None:
            print("[monitor] no reference parquet yet; run `make train` first. waiting...", flush=True)
            time.sleep(INTERVAL)
            continue

        current = current_window(client)
        if len(current) < MIN_ROWS:
            print(f"[monitor] current rows {len(current)} < {MIN_ROWS}; waiting for data...", flush=True)
            time.sleep(INTERVAL)
            continue

        summary, by_feature = compute_drift(reference, current)
        pred_rate, pred_mean = prediction_summary(client)
        ref_fraud_rate = float(reference["is_fraud"].mean()) if "is_fraud" in reference else 0.0
        missing_share = float(current[FEATURES].isna().mean().mean())

        client.insert(
            "dwh.drift_metrics",
            [[
                len(current), summary["n_features"], summary["n_drifted"], summary["drift_share"],
                summary["dataset_drift"], pred_rate, pred_mean, ref_fraud_rate, missing_share,
            ]],
            column_names=[
                "current_rows", "n_features", "n_drifted", "drift_share", "dataset_drift",
                "pred_fraud_rate", "pred_mean_proba", "ref_fraud_rate", "missing_share",
            ],
        )
        if by_feature:
            client.insert(
                "dwh.drift_by_feature", by_feature,
                column_names=["feature", "drift_score", "drifted"],
            )
        print(
            f"[monitor] rows={len(current)} drift_share={summary['drift_share']:.2f} "
            f"dataset_drift={summary['dataset_drift']} pred_fraud_rate={pred_rate:.3f}",
            flush=True,
        )
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
