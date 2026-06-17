"""ペルソナ別クライアント(入口).

env で挙動を変える1つのイメージを、compose で複数(client-jp / client-eu /
client-us / client-fraud)として起動する。各コンテナが独立ループで投げるので、
自然に「非同期・バラバラ」なストリームになる。

各イベントには地図可視化用の緯度経度(地域中心 + ジッタ)と、流入元を表す
source(ペルソナ名)を付与する。

env:
  SOURCE       ペルソナ名(= transactions.source)
  REGION       jp / eu / us / fraud(地域と座標、国の傾向)
  RATE         1秒あたりの平均イベント数(ポアソン的に揺らす)
  FRAUD_BIAS   不正発生確率への加算バイアス(0.0〜1.0)
  INGEST_URL   Ingest API のベースURL
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

# 地域ごとの (国, 緯度, 経度) 候補。ここを中心に少しジッタさせて地図に散らす。
REGIONS = {
    "jp":    [("JP", 35.68, 139.76), ("JP", 34.69, 135.50)],
    "eu":    [("GB", 51.51, -0.13), ("DE", 50.11, 8.68), ("FR", 48.85, 2.35)],
    "us":    [("US", 40.71, -74.00), ("US", 34.05, -118.24), ("US", 41.88, -87.63)],
    "fraud": [("NG", 9.06, 7.49), ("RU", 55.75, 37.62), ("BR", -23.55, -46.63)],
}
CATEGORIES = ["grocery", "electronics", "travel", "gaming", "jewelry"]
DEVICES = ["ios", "android", "web", "pos"]

SOURCE = os.environ.get("SOURCE", "client-jp")
REGION = os.environ.get("REGION", "jp")
RATE = float(os.environ.get("RATE", "1.0"))
FRAUD_BIAS = float(os.environ.get("FRAUD_BIAS", "0.0"))
INGEST_URL = os.environ.get("INGEST_URL", "http://ingest-api:8000")


def make_event() -> dict:
    country, lat, lon = random.choice(REGIONS.get(REGION, REGIONS["jp"]))
    device = random.choice(DEVICES)
    category = random.choice(CATEGORIES)
    amount = round(random.expovariate(1 / 80.0) + 1, 2)

    # 不正の起こりやすさ(sample1 と同じ相関 + ペルソナのバイアス)
    score = FRAUD_BIAS
    if amount > 300:
        score += 0.4
    if country in ("NG", "RU", "BR"):
        score += 0.2
    if device == "web":
        score += 0.15
    if category in ("electronics", "jewelry"):
        score += 0.15
    is_fraud = 1 if random.random() < min(score, 0.95) else 0

    return {
        "user_id": random.randint(1, 2000),
        "amount": amount,
        "merchant_category": category,
        "country": country,
        "device": device,
        # 地域中心から ±0.4 度ほどジッタさせる
        "latitude": round(lat + random.uniform(-0.4, 0.4), 5),
        "longitude": round(lon + random.uniform(-0.4, 0.4), 5),
        "source": SOURCE,
        "is_fraud": is_fraud,
    }


def post(event: dict) -> bool:
    data = json.dumps(event).encode()
    req = urllib.request.Request(
        f"{INGEST_URL}/transactions", data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 201
    except urllib.error.URLError as exc:
        print(f"[{SOURCE}] POST failed: {exc}", file=sys.stderr)
        return False


def main() -> None:
    print(f"[{SOURCE}] start: region={REGION} rate={RATE}/s fraud_bias={FRAUD_BIAS} -> {INGEST_URL}")
    sent = 0
    while True:
        if post(make_event()):
            sent += 1
            if sent % 50 == 0:
                print(f"[{SOURCE}] sent {sent}")
        # ポアソン的な到着間隔でバラバラに投げる
        time.sleep(random.expovariate(RATE) if RATE > 0 else 1.0)


if __name__ == "__main__":
    main()
