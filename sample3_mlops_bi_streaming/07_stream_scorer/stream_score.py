"""Spark Structured Streaming によるリアルタイム・スコアリング.

sample2 の「10秒ポーリング Scorer」を、Kafka を直接購読する常駐ストリーミングジョブに
置き換えたもの。Debezium が流す CDC イベント(oltp.public.transactions)を readStream し、
特徴量を作って最新モデルでスコアリングし、結果を ClickHouse の dwh.predictions へ
連続 append する。

生データの取り込み(dwh.transactions)は従来どおり ClickHouse の Kafka Engine が担当。
このジョブは推論だけを担う(同じトピックを別コンシューマグループで購読する)。

学習(Trainer)はバッチのまま。モデル更新は mtime 監視で foreachBatch 内で再ロードする。
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone

import clickhouse_connect
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "oltp.public.transactions")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")
META_PATH = os.environ.get("META_PATH", "/models/metadata.json")
CHECKPOINT = os.environ.get("CHECKPOINT", "/data/checkpoint")
TRIGGER = os.environ.get("TRIGGER", "5 seconds")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

FEATURES = ["merchant_category", "country", "device", "amount", "amount_log", "hour", "dow"]

# Debezium が unwrap した flat JSON のうち、推論に必要なフィールドの型。
EVENT_SCHEMA = StructType([
    StructField("tx_uuid", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("created_at", StringType()),
])

# foreachBatch はドライバ上で動くので、モデルと ClickHouse クライアントはここで保持する。
_state = {"model": None, "mtime": None, "version": "0", "client": None}


def ch_client():
    if _state["client"] is None:
        _state["client"] = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
        )
    return _state["client"]


def refresh_model() -> bool:
    """モデルが更新されていればロードし直す。ロード済みなら True。"""
    if not os.path.exists(MODEL_PATH):
        return False
    mtime = os.path.getmtime(MODEL_PATH)
    if mtime != _state["mtime"]:
        _state["model"] = joblib.load(MODEL_PATH)
        _state["mtime"] = mtime
        _state["version"] = str(int(mtime))
        print(f"[stream] loaded model version={_state['version']}", flush=True)
        log_model_run(_state["version"])
    return True


def log_model_run(version: str) -> None:
    try:
        import json
        with open(META_PATH) as f:
            meta = json.load(f)
    except (OSError, ValueError):
        return
    m = meta.get("metrics", {})
    raw = meta.get("trained_at")
    try:
        trained_at = datetime.fromisoformat(raw) if raw else datetime.now(timezone.utc)
    except (TypeError, ValueError):
        trained_at = datetime.now(timezone.utc)
    if trained_at.tzinfo is not None:
        trained_at = trained_at.astimezone(timezone.utc).replace(tzinfo=None)
    ch_client().insert(
        "dwh.model_runs",
        [[version, trained_at, int(m.get("n_train") or 0),
          int(m.get("n_test") or 0), float(m.get("roc_auc") or 0.0)]],
        column_names=["model_version", "trained_at", "n_train", "n_test", "roc_auc"],
    )
    print(f"[stream] logged model_run version={version} roc_auc={m.get('roc_auc')}", flush=True)


def process_batch(batch_df, epoch_id: int) -> None:
    if not refresh_model():
        print("[stream] model not found yet; run the trainer (make train)", flush=True)
        return

    pdf = batch_df.select("tx_uuid", *FEATURES).toPandas()
    if pdf.empty:
        return

    proba = _state["model"].predict_proba(pdf[FEATURES])[:, 1]
    rows = [
        [tx, float(p), int(p >= 0.5), _state["version"]]
        for tx, p in zip(pdf["tx_uuid"], proba)
    ]
    ch_client().insert(
        "dwh.predictions", rows,
        column_names=["tx_uuid", "fraud_probability", "pred_label", "model_version"],
    )
    print(f"[stream] epoch={epoch_id} scored {len(rows)} rows (version={_state['version']})", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("realtime-scorer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    events = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("tx_uuid").isNotNull())
    )

    # 学習時(Spark の dayofweek: 日=1..土=7)と同じ規則で特徴量を作る。
    features = (
        events.withColumn("ts", F.to_timestamp("created_at"))
        .withColumn("amount_log", F.log1p(F.col("amount")))
        .withColumn("hour", F.hour("ts"))
        .withColumn("dow", F.dayofweek("ts"))
        .select("tx_uuid", *FEATURES)
    )

    query = (
        features.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"[stream] streaming started: topic={KAFKA_TOPIC} trigger='{TRIGGER}'", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
