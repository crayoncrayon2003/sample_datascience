"""Spark 前処理(学習データ生成の集計).

**ライブの実時間データだけ**を学習に使う(偽時間・合成データは廃止)。
道路網エージェント feed.py が投入した dwh.vehicle_positions(車両位置の生データ)を JDBC で読み、
1 観測ごとに渋滞ラベルを付けて parquet に書き出す。stream-scorer 用のモデルはこれで学習する。

  - ラベル(target): 速度から <12=congested(2), <24=slow(1), それ以外=smooth(0)
  - 特徴量: road_type(道路種別) / maxspeed / sim_hour(実時刻の hour) / sim_dow
    リンク固有の癖(link_factor)は特徴量に無い=モデルから見えない誤差源(精度が 1.0 に張り付かない)

> 注意: 学習に使えるのは「実際に車が走った道路種別 × その時に経過した時間帯」だけ。
>       ランダムウォークは交通量に比例して偏り、全時間帯も実時間ぶんしか溜まらないため、
>       学習データは少なく不均衡になり、予測精度は上がりにくい(本サンプルはそれを許容する)。
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/dwh")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/data/features/links")


def main() -> None:
    spark = SparkSession.builder.appName("congestion-preprocess").getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "dwh.vehicle_positions")
        .option("user", "default")
        .option("password", "")
        .load()
    )

    # CDC 再送による重複を排除(pos_uuid 単位で id が最大の行=最新を残す)。
    w = Window.partitionBy("pos_uuid").orderBy(F.col("id").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 1 観測ずつにラベルを付ける(平均してからラベル化しない)。
    # 同じ (road_type, hour) でも link_factor 由来のばらつきでラベルが混じるので、
    # モデルは「期待される渋滞レベル」を学び、隠れ要因の誤差は残る(精度 < 1.0)。
    congestion = (
        F.when(F.col("speed_kmh") < 12, F.lit(2))
        .when(F.col("speed_kmh") < 24, F.lit(1))
        .otherwise(F.lit(0))
    )
    features = deduped.withColumn("congestion", congestion).select(
        "road_type", "maxspeed", "sim_hour", "sim_dow", "congestion"
    )

    count = features.count()
    print(f"[preprocess] writing {count} labeled samples to {OUTPUT_PATH}")
    features.write.mode("overwrite").parquet(OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
