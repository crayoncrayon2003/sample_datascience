"""学習データ生成(バッチ).

ライブフィード(feed.py)とは別物。動く車も偽の時計も無く、
「全時間帯 × 道路種別 × 制限速度」を直接スイープして、共有の渋滞モデル(congestion.py)で
速度サンプルを生成し、ClickHouse の dwh.vehicle_history に投入する。
リンク固有係数(link_factor)は毎サンプルでランダムに引く=観測できない隠れ要因。

Spark 前処理はこの vehicle_history を集計して学習データにする(ライブの生データは使わない)。
これで「時間を偽って高速に回す」処理がライブ経路から消え、学習用データ生成に局在化する。

env: CLICKHOUSE_HOST / CLICKHOUSE_PORT / SAMPLES_PER_CELL / DAYS
"""
from __future__ import annotations

import os
import random

import clickhouse_connect

import congestion

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
SAMPLES_PER_CELL = int(os.environ.get("SAMPLES_PER_CELL", "30"))
DAYS = int(os.environ.get("DAYS", "7"))

# 代表的な道路種別と制限速度(OSM の highway に対応)。
# OSM の *_link もライブ側で流れるため、学習側にも入れて未知カテゴリ化を避ける。
ROAD_TYPES = [
    ("residential", 40), ("unclassified", 40), ("service", 30),
    ("tertiary", 40), ("tertiary_link", 40),
    ("secondary", 50), ("secondary_link", 40),
    ("primary", 60), ("primary_link", 50),
    ("trunk", 60), ("trunk_link", 50),
    ("motorway", 100), ("motorway_link", 60),
]

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS dwh.vehicle_history
(
    road_type String,
    maxspeed  Float64,
    sim_hour  Int16,
    sim_dow   Int16,
    speed_kmh Float64
)
ENGINE = MergeTree
ORDER BY (road_type, sim_hour, sim_dow)
"""


def main() -> None:
    rng = random.Random(42)
    rows = []
    for hour in range(24):
        for dow in range(1, DAYS + 1):
            for road_type, maxspeed in ROAD_TYPES:
                for _ in range(SAMPLES_PER_CELL):
                    lf = rng.uniform(0.55, 1.0)              # 隠れ要因(link_factor)
                    sp = congestion.edge_speed(maxspeed, road_type, hour, lf, rng)
                    rows.append([road_type, float(maxspeed), hour, dow, round(sp, 2)])

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
    )
    client.command("CREATE DATABASE IF NOT EXISTS dwh")
    client.command(CREATE_SQL)
    client.command("TRUNCATE TABLE dwh.vehicle_history")
    client.insert(
        "dwh.vehicle_history", rows,
        column_names=["road_type", "maxspeed", "sim_hour", "sim_dow", "speed_kmh"],
    )
    print(f"[trainsim] generated {len(rows)} samples "
          f"({24}h x {DAYS}d x {len(ROAD_TYPES)}types x {SAMPLES_PER_CELL}) -> dwh.vehicle_history")


if __name__ == "__main__":
    main()
