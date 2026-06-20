-- OLTP スキーマ:道路網エージェントの車両位置イベント(道路リンク単位)
CREATE TABLE IF NOT EXISTS vehicle_positions (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pos_uuid     TEXT             NOT NULL,
    vehicle_id   TEXT             NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    speed_kmh    DOUBLE PRECISION NOT NULL,
    link_id      TEXT             NOT NULL,
    road_type    TEXT             NOT NULL,
    maxspeed     DOUBLE PRECISION NOT NULL,
    sim_hour     SMALLINT         NOT NULL,
    sim_dow      SMALLINT         NOT NULL,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
);

ALTER TABLE vehicle_positions REPLICA IDENTITY FULL;
