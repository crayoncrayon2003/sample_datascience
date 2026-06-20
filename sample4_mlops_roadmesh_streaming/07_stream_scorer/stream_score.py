"""Spark Structured Streaming によるリアルタイム渋滞予測.

Kafka の車両位置イベント(oltp.public.vehicle_positions)を直接購読し、区間ごとの渋滞レベル
(0=smooth / 1=slow / 2=congested)を最新モデルで予測して dwh.congestion_pred に書き戻す。
Grafana はこの結果を地図に描画する(車両位置を予測渋滞で色分け)。

比較用に、実際の速度から求めた渋滞ラベル(actual)も一緒に書き込む。
学習はバッチ(make train)。モデル更新は mtime 監視で自動反映する。
"""
from __future__ import annotations

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
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "oltp.public.vehicle_positions")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")
META_PATH = os.environ.get("META_PATH", "/models/metadata.json")
CHECKPOINT = os.environ.get("CHECKPOINT", "/data/checkpoint")
TRIGGER = os.environ.get("TRIGGER", "5 seconds")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

FEATURES = ["road_type", "maxspeed", "sim_hour", "sim_dow"]

EVENT_SCHEMA = StructType([
    StructField("pos_uuid", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("link_id", StringType()),
    StructField("road_type", StringType()),
    StructField("maxspeed", DoubleType()),
    StructField("sim_hour", IntegerType()),
    StructField("sim_dow", IntegerType()),
    StructField("created_at", StringType()),
])

_state = {"model": None, "mtime": None, "version": "0", "client": None}


def ch_client():
    if _state["client"] is None:
        _state["client"] = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
        )
    return _state["client"]


def actual_label(speed: float) -> int:
    if speed < 12:
        return 2
    if speed < 24:
        return 1
    return 0


def refresh_model() -> bool:
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
          int(m.get("n_test") or 0), float(m.get("accuracy") or 0.0)]],
        column_names=["model_version", "trained_at", "n_train", "n_test", "accuracy"],
    )
    print(f"[stream] logged model_run version={version} accuracy={m.get('accuracy')}", flush=True)


def process_batch(batch_df, epoch_id: int) -> None:
    if not refresh_model():
        print("[stream] model not found yet; run the trainer (make train)", flush=True)
        return

    pdf = batch_df.select(
        "pos_uuid", "vehicle_id", "latitude", "longitude", "speed_kmh",
        "link_id", "road_type", "maxspeed", "sim_hour", "sim_dow", "created_at",
    ).toPandas()
    if pdf.empty:
        return

    proba = _state["model"].predict_proba(pdf[FEATURES])
    pred = proba.argmax(axis=1)
    # event_time は DateTime64 列なので datetime オブジェクトに変換して渡す
    # (clickhouse-connect は ISO 文字列を受け付けない。Z 付き ISO も pandas なら堅牢に解釈)
    event_dt = pd.to_datetime(pdf["created_at"], utc=True, errors="coerce").dt.tz_localize(None)
    rows = []
    for i in range(len(pdf)):
        r = pdf.iloc[i]
        rows.append([
            r["pos_uuid"], r["vehicle_id"],
            float(r["latitude"]), float(r["longitude"]), str(r["link_id"]), str(r["road_type"]),
            int(r["sim_hour"]),
            int(pred[i]), float(proba[i].max()), actual_label(float(r["speed_kmh"])),
            _state["version"], event_dt.iloc[i].to_pydatetime(),
        ])
    ch_client().insert(
        "dwh.congestion_pred", rows,
        column_names=[
            "pos_uuid", "vehicle_id", "latitude", "longitude", "link_id", "road_type", "sim_hour",
            "pred_congestion", "pred_proba", "actual_congestion", "model_version", "event_time",
        ],
    )
    print(f"[stream] epoch={epoch_id} scored {len(rows)} rows (version={_state['version']})", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("realtime-congestion").getOrCreate()
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
        .filter(F.col("pos_uuid").isNotNull())
    )

    query = (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"[stream] streaming started: topic={KAFKA_TOPIC} trigger='{TRIGGER}'", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
