"""Spark 前処理(学習データ生成の集計).

学習データ生成(trainsim.py / make traindata)が作った dwh.vehicle_history を JDBC で読み、
(road_type, maxspeed, sim_hour, sim_dow) ごとに平均速度を集計してラベルを付ける。
ライブの vehicle_positions は使わない(可視化専用に分離した)。

  - ラベル(target): 平均速度から <12=congested(2), <24=slow(1), それ以外=smooth(0)
  - 特徴量: road_type(道路種別) / maxspeed / sim_hour / sim_dow
    リンク固有の癖(link_factor)は特徴量に無い=モデルから見えない誤差源(精度が 1.0 に張り付かない)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/dwh")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/features/links")


def main() -> None:
    spark = SparkSession.builder.appName("congestion-preprocess").getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "dwh.vehicle_history")
        .option("user", "default")
        .option("password", "")
        .load()
    )

    # サンプル1件ずつにラベルを付ける(平均してからラベル化しない)。
    # 平均すると隠れ要因 link_factor のばらつきが消えて自己充足的になるため、
    # 同じ (road_type, hour) でもサンプルごとに渋滞/非渋滞が混じる状態のまま学習する
    # → モデルは「期待される渋滞レベル」を学び、link_factor 由来の誤差は残る(精度 < 1.0)。
    congestion = (
        F.when(F.col("speed_kmh") < 12, F.lit(2))
        .when(F.col("speed_kmh") < 24, F.lit(1))
        .otherwise(F.lit(0))
    )
    features = df.withColumn("congestion", congestion).select(
        "road_type", "maxspeed", "sim_hour", "sim_dow", "congestion"
    )

    count = features.count()
    print(f"[preprocess] writing {count} labeled samples to {OUTPUT_PATH}")
    features.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
