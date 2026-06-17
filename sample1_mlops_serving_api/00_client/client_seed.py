"""Client(投入側).

パイプラインの入口。Ingest API に取引イベントを POST してデータを流し込む。
出口の 08_client/client_predict.py と対になる存在。
fraud(不正)取引は「高額・特定の国・特定デバイス」で発生しやすい、という
分かりやすい相関を持たせて生成しているので、学習後に Serving API が
それらしい確率を返すようになる。

使い方:
    python 00_client/client_seed.py --n 2000 --url http://localhost:8000
"""
from __future__ import annotations

import argparse
import random
import sys
import urllib.error
import urllib.request

CATEGORIES = ["grocery", "electronics", "travel", "gaming", "jewelry"]
COUNTRIES = ["JP", "US", "GB", "NG", "RU"]
DEVICES = ["ios", "android", "web", "pos"]


def make_event() -> dict:
    country = random.choice(COUNTRIES)
    device = random.choice(DEVICES)
    category = random.choice(CATEGORIES)
    amount = round(random.expovariate(1 / 80.0) + 1, 2)

    # 不正の起こりやすさ(意図的に相関を仕込む)
    score = 0.0
    if amount > 300:
        score += 0.4
    if country in ("NG", "RU"):
        score += 0.3
    if device == "web":
        score += 0.15
    if category in ("electronics", "jewelry"):
        score += 0.15
    is_fraud = 1 if random.random() < min(score, 0.95) else 0

    return {
        "user_id": random.randint(1, 500),
        "amount": amount,
        "merchant_category": category,
        "country": country,
        "device": device,
        "is_fraud": is_fraud,
    }


def post(url: str, event: dict) -> bool:
    import json

    data = json.dumps(event).encode()
    req = urllib.request.Request(
        f"{url}/transactions", data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 201
    except urllib.error.URLError as exc:
        print(f"POST failed: {exc}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()

    ok = 0
    for i in range(args.n):
        if post(args.url, make_event()):
            ok += 1
        if (i + 1) % 200 == 0:
            print(f"  sent {i + 1}/{args.n} ...")
    print(f"done: {ok}/{args.n} events ingested")


if __name__ == "__main__":
    main()
