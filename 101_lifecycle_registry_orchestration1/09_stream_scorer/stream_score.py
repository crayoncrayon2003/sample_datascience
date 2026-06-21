"""Spark Structured Streaming によるリアルタイム・スコアリング.

10秒ポーリング型の Scorer に代えて、Kafka を直接購読する常駐ストリーミングジョブとして
実装したもの。Debezium が流す CDC イベント(oltp.public.transactions)を readStream し、
特徴量を作って最新モデルでスコアリングし、結果を ClickHouse の dwh.predictions へ
連続 append する。

生データの取り込み(dwh.transactions)は従来どおり ClickHouse の Kafka Engine が担当。
このジョブは推論だけを担う(同じトピックを別コンシューマグループで購読する)。

学習(Trainer)はバッチのまま。004 までと違い、モデルは **MLflow Model Registry の
alias `champion`** から解決する(`models:/fraud@champion`)。foreachBatch のたびに現在の
champion version を確認し、変わっていれば無停止で再ロードする(旧版の mtime 監視の正統進化)。
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import clickhouse_connect
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "oltp.public.transactions")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MODEL_NAME", "fraud")
MODEL_ALIAS = os.environ.get("MODEL_ALIAS", "champion")
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

# foreachBatch はドライバ上で動くので、モデルと各クライアントはここで保持する。
_state = {"model": None, "version": None, "client": None, "mlflow": None}


def ch_client():
    if _state["client"] is None:
        _state["client"] = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
        )
    return _state["client"]


def mlflow_client() -> MlflowClient:
    if _state["mlflow"] is None:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        _state["mlflow"] = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    return _state["mlflow"]


def refresh_model() -> bool:
    """Registry の @champion を確認し、version が変わっていれば再ロードする。

    champion がまだ居ない(昇格前)なら False。ロード済みなら True。
    """
    client = mlflow_client()
    try:
        mv = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    except Exception:  # noqa: BLE001  (champion 未設定)
        return _state["model"] is not None  # 旧モデルがあれば使い続ける

    if mv.version != _state["version"]:
        uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
        _state["model"] = mlflow.sklearn.load_model(uri)
        _state["version"] = mv.version
        print(f"[stream] loaded {MODEL_NAME} v{mv.version} from @{MODEL_ALIAS}", flush=True)
        log_model_run(mv)
    return True


def log_model_run(mv) -> None:
    """champion が切り替わったタイミングで、その version の指標を dwh.model_runs に記録。"""
    try:
        run = mlflow_client().get_run(mv.run_id)
        m = run.data.metrics
        trained_at = datetime.fromtimestamp(run.info.start_time / 1000, timezone.utc).replace(tzinfo=None)
    except Exception:  # noqa: BLE001
        m, trained_at = {}, datetime.now(timezone.utc).replace(tzinfo=None)
    ch_client().insert(
        "dwh.model_runs",
        [[str(mv.version), trained_at, int(m.get("n_train") or 0),
          int(m.get("n_test") or 0), float(m.get("roc_auc") or 0.0)]],
        column_names=["model_version", "trained_at", "n_train", "n_test", "roc_auc"],
    )
    print(f"[stream] logged model_run v{mv.version} roc_auc={m.get('roc_auc')}", flush=True)


def process_batch(batch_df, epoch_id: int) -> None:
    if not refresh_model():
        print("[stream] no @champion yet; run `make train` to register & promote", flush=True)
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
