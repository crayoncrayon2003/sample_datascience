"""Ingest API.

Client から取引イベントを受け取り、OLTP(PostgreSQL)へ INSERT するだけのサービス。
ここに書き込まれた行が WAL を通じて Debezium に拾われ、パイプライン下流へ流れていく。
"""
from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://app:app@postgres:5432/oltp"
)

app = FastAPI(title="Ingest API", version="1.0.0")


class Transaction(BaseModel):
    user_id: int
    amount: float = Field(gt=0)
    merchant_category: str
    country: str
    device: str
    # ラベル。実運用では後追いで付くが、サンプルでは生成時に付与する
    is_fraud: int = Field(default=0, ge=0, le=1)


@contextmanager
def get_conn():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        yield conn


@app.get("/health")
def health():
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/transactions", status_code=201)
def ingest(tx: Transaction):
    tx_uuid = str(uuid.uuid4())
    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO transactions
                    (tx_uuid, user_id, amount, merchant_category, country, device, is_fraud)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tx_uuid,
                    tx.user_id,
                    tx.amount,
                    tx.merchant_category,
                    tx.country,
                    tx.device,
                    tx.is_fraud,
                ),
            )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
    return {"tx_uuid": tx_uuid}
