"""Client(推論側).

パイプラインの出口。Serving API に特徴量を投げて fraud 確率を受け取るクライアント。
入口の 00_client/client_seed.py と対になる存在。

使い方:
    python 08_client/client_predict.py --n 5 --url http://localhost:8001
    python 08_client/client_predict.py --amount 450 --country RU --device web
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import urllib.error
import urllib.request

CATEGORIES = ["grocery", "electronics", "travel", "gaming", "jewelry"]
COUNTRIES = ["JP", "US", "GB", "NG", "RU"]
DEVICES = ["ios", "android", "web", "pos"]


def make_features() -> dict:
    """client_seed と同じ分布でランダムな取引特徴量を作る。"""
    return {
        "amount": round(random.expovariate(1 / 80.0) + 1, 2),
        "merchant_category": random.choice(CATEGORIES),
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "hour": random.randint(0, 23),
        "dow": random.randint(1, 7),
    }


def predict(url: str, features: dict) -> dict | None:
    data = json.dumps(features).encode()
    req = urllib.request.Request(
        f"{url}/predict", data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        print(f"HTTP {exc.code}: {exc.read().decode()}", file=sys.stderr)
    except urllib.error.URLError as exc:
        print(f"request failed: {exc}", file=sys.stderr)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=5, help="ランダム生成して投げる件数")
    parser.add_argument("--url", default="http://localhost:8001")
    # 単発で具体的な値を指定したい場合
    parser.add_argument("--amount", type=float)
    parser.add_argument("--merchant_category", choices=CATEGORIES)
    parser.add_argument("--country", choices=COUNTRIES)
    parser.add_argument("--device", choices=DEVICES)
    parser.add_argument("--hour", type=int)
    parser.add_argument("--dow", type=int)
    args = parser.parse_args()

    manual = any(
        v is not None
        for v in (
            args.amount,
            args.merchant_category,
            args.country,
            args.device,
            args.hour,
            args.dow,
        )
    )

    if manual:
        base = make_features()
        for key in ("amount", "merchant_category", "country", "device", "hour", "dow"):
            val = getattr(args, key)
            if val is not None:
                base[key] = val
        result = predict(args.url, base)
        print(json.dumps({"request": base, "response": result}, ensure_ascii=False))
        return

    for _ in range(args.n):
        features = make_features()
        result = predict(args.url, features)
        if result is None:
            continue
        print(
            f"amount={features['amount']:>8.2f}  {features['country']:>2}  "
            f"{features['device']:<7}  {features['merchant_category']:<11}"
            f" -> fraud_prob={result['fraud_probability']:.3f}  is_fraud={result['is_fraud']}"
        )


if __name__ == "__main__":
    main()
