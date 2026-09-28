"""Spark 前処理ジョブ.

ClickHouse(DWH)の dwh.transactions を JDBC で読み、学習用の特徴量へ整形して
parquet を /data/features に書き出す。lat/lon/source は可視化用なので特徴量には使わない。
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/dwh")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/features/transactions")


def main() -> None:
    spark = SparkSession.builder.appName("transactions-preprocess").getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "dwh.transactions")
        .option("user", "default")
        .option("password", "")
        .load()
    )

    # tx_uuid 毎に id が最大の行(最新)を残す
    w = Window.partitionBy("tx_uuid").orderBy(F.col("id").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    features = (
        deduped.withColumn("amount_log", F.log1p(F.col("amount")))
        .withColumn("hour", F.hour(F.col("created_at")))
        .withColumn("dow", F.dayofweek(F.col("created_at")))
        .select(
            "tx_uuid", "user_id", "amount", "amount_log",
            "merchant_category", "country", "device", "hour", "dow", "is_fraud",
        )
    )

    count = features.count()
    print(f"[preprocess] writing {count} rows to {OUTPUT_PATH}")
    features.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
