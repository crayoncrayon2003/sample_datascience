"""道路リンクの固定座標(中点)と進行方位(bearing)を ClickHouse に投入する.

矢印1本/リンク表示のための次元テーブル dwh.link_geo を作る。
link_id は feed と同じ graphml_road.load_graph で作るので dwh.vehicle_positions と一致し、
ダッシュボードの渋滞集計(link_id 単位)と JOIN できる。

  mid_lat/mid_lon … リンク線形の中点(時間で動かない固定座標。矢印の置き場所)
  bearing         … 進行方向(source→target)の方位。中点が乗る区間の向き(度・北=0・時計回り)

env: GRAPHML / CLICKHOUSE_HOST / CLICKHOUSE_PORT
"""
from __future__ import annotations

import math
import os
import sys

import clickhouse_connect

import graphml_road

GRAPHML = os.environ.get("GRAPHML") or "/data/road/road_drive.graphml"
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS dwh.link_geo
(
    link_id String,
    mid_lat Float64,
    mid_lon Float64,
    bearing Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY link_id
"""


def haversine(a, b) -> float:
    R = 6371000.0
    (la1, lo1), (la2, lo2) = a, b
    p1, p2 = math.radians(la1), math.radians(la2)
    dp, dl = math.radians(la2 - la1), math.radians(lo2 - lo1)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(x))


def bearing(a, b) -> float:
    """a から b への初期方位(度・北=0・東=90・時計回り)。"""
    (la1, lo1), (la2, lo2) = a, b
    p1, p2 = math.radians(la1), math.radians(la2)
    dl = math.radians(lo2 - lo1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def midpoint_and_bearing(poly):
    """リンク線形(polyline)の中点座標と、その中点が乗る区間の進行方位を返す。"""
    segs = [(poly[i], poly[i + 1], haversine(poly[i], poly[i + 1])) for i in range(len(poly) - 1)]
    total = sum(s[2] for s in segs)
    half = total / 2.0
    d = 0.0
    for i, (a, b, seg) in enumerate(segs):
        if d + seg >= half or i == len(segs) - 1:
            f = min(max((half - d) / seg, 0.0), 1.0) if seg > 0 else 0.0
            mid = (a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)
            return mid, bearing(a, b)
        d += seg
    return poly[0], bearing(poly[0], poly[-1])


def main() -> None:
    if not os.path.exists(GRAPHML):
        print(f"[linkgeo] graphml not found: {GRAPHML}. run `make data` first.", file=sys.stderr)
        sys.exit(1)

    nodes, adj = graphml_road.load_graph(GRAPHML)
    rows = []
    seen = set()
    for edges in adj.values():
        for target, poly, link_id, road_type, maxspeed in edges:
            if link_id in seen or len(poly) < 2:
                continue
            seen.add(link_id)
            mid, brg = midpoint_and_bearing(poly)
            rows.append([link_id, float(mid[0]), float(mid[1]), float(brg)])

    if not rows:
        print(f"[linkgeo] no links found in {GRAPHML}", file=sys.stderr)
        sys.exit(1)

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username="default", password=""
    )
    client.command("CREATE DATABASE IF NOT EXISTS dwh")
    client.command(CREATE_SQL)
    client.command("TRUNCATE TABLE dwh.link_geo")
    client.insert(
        "dwh.link_geo", rows,
        column_names=["link_id", "mid_lat", "mid_lon", "bearing"],
    )
    print(f"[linkgeo] inserted {len(rows)} links into dwh.link_geo (from {GRAPHML})")


if __name__ == "__main__":
    main()
