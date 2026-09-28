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
  DRIFT_AFTER  これ秒経過後に分布を変える(0=しない)。ドリフト監視のデモ用に、
               高額化・カテゴリ偏り・高リスク国増加・不正増加へ徐々にシフトさせる。
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
DRIFT_AFTER = float(os.environ.get("DRIFT_AFTER", "0"))  # 0 = ドリフトさせない

_START = time.time()

# ドリフト後に偏らせる分布(高額・高リスクカテゴリ/国・不正増)。
DRIFT_CATEGORIES = ["electronics", "jewelry", "electronics", "gaming"]
DRIFT_COUNTRIES = [("NG", 9.06, 7.49), ("RU", 55.75, 37.62), ("BR", -23.55, -46.63)]


def drift_strength() -> float:
    """ドリフトの進み具合 0.0〜1.0。DRIFT_AFTER 経過後、5 分かけて 1.0 まで上げる。"""
    if DRIFT_AFTER <= 0:
        return 0.0
    elapsed = time.time() - _START - DRIFT_AFTER
    if elapsed <= 0:
        return 0.0
    return min(1.0, elapsed / 300.0)


def make_event() -> dict:
    d = drift_strength()  # 0.0〜1.0(時間とともに上昇)

    # 国: ドリフトが進むほど高リスク国を選びやすくする
    if d > 0 and random.random() < d:
        country, lat, lon = random.choice(DRIFT_COUNTRIES)
    else:
        country, lat, lon = random.choice(REGIONS.get(REGION, REGIONS["jp"]))
    device = random.choice(DEVICES)
    # カテゴリ: ドリフトが進むほど電子/宝飾に偏らせる
    category = random.choice(DRIFT_CATEGORIES if (d > 0 and random.random() < d) else CATEGORIES)
    # 金額: 平均 80 → ドリフトで最大 +220 ぶん高額側へシフト
    amount = round(random.expovariate(1 / (80.0 + 220.0 * d)) + 1, 2)

    # 不正の起こりやすさ(金額・国・デバイス等との相関 + ペルソナのバイアス + ドリフト)
    score = FRAUD_BIAS + 0.25 * d
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
