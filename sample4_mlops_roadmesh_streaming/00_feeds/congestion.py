"""共有の渋滞モデル(合成 ground-truth).

ライブフィード(feed.py)と学習データ生成(trainsim.py)の両方がこれを使い、
速度・渋滞ラベルの決め方を1か所に統一する。

  speed = maxspeed × 時間帯係数(hour) × 道路種別係数(ラッシュ時) × link_factor × ノイズ

link_factor は「リンク固有の慢性的な混みやすさ」。モデルの特徴量には入れない隠れ要因として残し、
予測精度が 1.0 に張り付かない(自己充足的でない)ようにするためのもの。
"""
from __future__ import annotations

import random
import zlib

RUSH = (7, 8, 9, 17, 18, 19)

# 道路種別ごとの「ラッシュ時の混みやすさ」(小さいほど遅くなる)
ROADTYPE_RUSH = {
    "motorway": 0.70, "motorway_link": 0.70,
    "trunk": 0.65, "trunk_link": 0.65,
    "primary": 0.60, "primary_link": 0.60,
    "secondary": 0.68, "secondary_link": 0.68,
    "tertiary": 0.78, "tertiary_link": 0.78,
    "residential": 0.92, "unclassified": 0.86, "service": 0.95,
}


def hour_factor(hour: int) -> float:
    if hour in RUSH:
        return 0.50
    if hour in (6, 10, 16, 20):
        return 0.7
    if hour in (0, 1, 2, 3, 4):
        return 1.1
    return 1.0


def link_factor(link_id: str) -> float:
    """リンクIDから決まる固定の混みやすさ(0.55〜1.0)。ライブフィード用。"""
    return 0.55 + 0.45 * ((zlib.crc32(link_id.encode()) % 1000) / 1000.0)


def edge_speed(maxspeed: float, road_type: str, hour: int, lf: float, rng=random) -> float:
    """渋滞 ground-truth の走行速度(km/h)。lf = link_factor(隠れ要因)。"""
    f = hour_factor(hour) * lf * rng.uniform(0.85, 1.15)
    if hour in RUSH:
        f *= ROADTYPE_RUSH.get(road_type, 0.7)
    return max(3.0, maxspeed * f)


def label(speed: float) -> int:
    """速度 → 渋滞レベル。0=smooth / 1=slow / 2=congested。"""
    if speed < 12:
        return 2
    if speed < 24:
        return 1
    return 0
