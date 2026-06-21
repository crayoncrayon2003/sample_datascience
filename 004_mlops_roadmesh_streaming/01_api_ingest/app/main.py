"""Ingest API.

道路網エージェントの車両位置イベントを受け取り、OLTP(PostgreSQL)へ INSERT する。
WAL → Debezium → Kafka → ClickHouse へ流れ、地図表示と道路リンク渋滞予測に使われる。
"""
from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

import psycopg
from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@postgres:5432/oltp")

app = FastAPI(title="Road-mesh Ingest API", version="1.0.0")


class VehiclePosition(BaseModel):
    vehicle_id: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    speed_kmh: float = Field(ge=0)
    link_id: str
    road_type: str
    maxspeed: float = Field(ge=0)
    sim_hour: int = Field(ge=0, le=23)
    sim_dow: int = Field(ge=1, le=7)


INSERT_SQL = """
INSERT INTO vehicle_positions
    (pos_uuid, vehicle_id, latitude, longitude, speed_kmh,
     link_id, road_type, maxspeed, sim_hour, sim_dow)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


@contextmanager
def get_conn():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        yield conn


def position_row(pos_uuid: str, pos: VehiclePosition) -> tuple:
    return (
        pos_uuid, pos.vehicle_id, pos.latitude, pos.longitude, pos.speed_kmh,
        pos.link_id, pos.road_type, pos.maxspeed, pos.sim_hour, pos.sim_dow,
    )


@app.get("/health")
def health():
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/positions", status_code=201)
def ingest(pos: VehiclePosition):
    pos_uuid = str(uuid.uuid4())
    try:
        with get_conn() as conn:
            conn.execute(INSERT_SQL, position_row(pos_uuid, pos))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
    return {"pos_uuid": pos_uuid}


@app.post("/positions/batch", status_code=201)
def ingest_batch(positions: list[VehiclePosition] = Body(min_length=1, max_length=5000)):
    rows = [position_row(str(uuid.uuid4()), pos) for pos in positions]
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, rows)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))
    return {"inserted": len(rows)}
