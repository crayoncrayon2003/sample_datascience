-- =============================================================================
-- ClickHouse : Sink(Kafka Engine)+ DWH(MergeTree)+ 可視化/予測用テーブル
--
--   vehicle_positions … 車両位置の生データ(道路リンク link_id / 道路種別 / 制限速度 付き)
--   congestion_pred   … stream-scorer が書き戻すリンク渋滞予測
--   model_runs        … 再学習ごとの指標(精度推移の可視化用)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.positions_queue
(
    id          Int64,
    pos_uuid    String,
    vehicle_id  String,
    latitude    Float64,
    longitude   Float64,
    speed_kmh   Float64,
    link_id     String,
    road_type   String,
    maxspeed    Float64,
    sim_hour    Int16,
    sim_dow     Int16,
    created_at  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'oltp.public.vehicle_positions',
    kafka_group_name = 'clickhouse_sink',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1,
    date_time_input_format = 'best_effort';

CREATE TABLE IF NOT EXISTS dwh.vehicle_positions
(
    id          Int64,
    pos_uuid    String,
    vehicle_id  String,
    latitude    Float64,
    longitude   Float64,
    speed_kmh   Float64,
    link_id     String,
    road_type   String,
    maxspeed    Float64,
    sim_hour    Int16,
    sim_dow     Int16,
    created_at  DateTime64(3),
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(id)
ORDER BY pos_uuid;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.vehicle_positions_mv TO dwh.vehicle_positions AS
SELECT
    id, pos_uuid, vehicle_id, latitude, longitude, speed_kmh,
    link_id, road_type, maxspeed, sim_hour, sim_dow,
    parseDateTime64BestEffort(created_at) AS created_at
FROM dwh.positions_queue;

-- リンク渋滞予測(stream-scorer が書き戻す)。0=smooth, 1=slow, 2=congested
CREATE TABLE IF NOT EXISTS dwh.congestion_pred
(
    pos_uuid          String,
    vehicle_id        String,
    latitude          Float64,
    longitude         Float64,
    link_id           String,
    road_type         String,
    sim_hour          Int16,
    pred_congestion   Int8,
    pred_proba        Float64,
    actual_congestion Int8,
    model_version     String,
    event_time        DateTime64(3),
    scored_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(scored_at)
ORDER BY pos_uuid;

CREATE TABLE IF NOT EXISTS dwh.model_runs
(
    model_version String,
    trained_at    DateTime64(3),
    n_train       UInt64,
    n_test        UInt64,
    accuracy      Float64
)
ENGINE = ReplacingMergeTree(trained_at)
ORDER BY model_version;

-- リンクの固定座標(中点)と進行方位(bearing)。矢印1本/リンク表示用の次元テーブル。
-- make linkgeo(09_linkgeo)が graphml から計算して投入する。link_id は vehicle_positions と一致。
CREATE TABLE IF NOT EXISTS dwh.link_geo
(
    link_id String,
    mid_lat Float64,
    mid_lon Float64,
    bearing Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY link_id;
