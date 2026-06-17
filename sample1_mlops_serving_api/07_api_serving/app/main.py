"""Serving API.

Trainer が保存したモデル(/models/model.joblib)をロードして予測を返す。
モデルは前処理(OneHot 等)込みの sklearn Pipeline なので、入力特徴量を渡すだけでよい。
amount_log は学習時に Spark で作った派生特徴量なので、ここでも amount から計算する。
"""
from __future__ import annotations

import math
import os

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.joblib")

app = FastAPI(title="Serving API", version="1.0.0")
_model = None


def get_model():
    global _model
    if _model is None:
        if not os.path.exists(MODEL_PATH):
            raise HTTPException(
                status_code=503,
                detail=f"model not found at {MODEL_PATH}; run the trainer first",
            )
        _model = joblib.load(MODEL_PATH)
    return _model


class Features(BaseModel):
    amount: float = Field(gt=0)
    merchant_category: str
    country: str
    device: str
    hour: int = Field(ge=0, le=23)
    dow: int = Field(ge=1, le=7)  # ClickHouse dayofweek 互換 (1=日 .. 7=土)


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": os.path.exists(MODEL_PATH)}


@app.post("/predict")
def predict(feat: Features):
    model = get_model()
    row = {
        "merchant_category": feat.merchant_category,
        "country": feat.country,
        "device": feat.device,
        "amount": feat.amount,
        "amount_log": math.log1p(feat.amount),
        "hour": feat.hour,
        "dow": feat.dow,
    }
    X = pd.DataFrame([row])
    proba = float(model.predict_proba(X)[0, 1])
    return {"fraud_probability": round(proba, 4), "is_fraud": int(proba >= 0.5)}
