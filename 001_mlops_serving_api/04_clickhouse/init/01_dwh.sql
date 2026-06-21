-- =============================================================================
-- ClickHouse : Sink (Kafka Engine) + DWH (MergeTree) + Materialized View
--
--   Kafka topic  oltp.public.transactions   (Debezium が unwrap した flat JSON)
--        │
--        ▼  Kafka Engine テーブル = 「ClickHouse Sink」
--   dwh.transactions_queue
--        │
--        ▼  Materialized View
--   dwh.transactions  (MergeTree / DWH 本体)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dwh;

-- ---------------------------------------------------------------------------
-- Sink : Kafka からイベントを読み込むキュー用テーブル
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.transactions_queue
(
    id                Int64,
    tx_uuid           String,
    user_id           Int32,
    amount            Float64,
    merchant_category String,
    country           String,
    device            String,
    is_fraud          UInt8,
    created_at        String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'oltp.public.transactions',
    kafka_group_name = 'clickhouse_sink',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    input_format_skip_unknown_fields = 1,
    date_time_input_format = 'best_effort';

-- ---------------------------------------------------------------------------
-- DWH 本体 : ReplacingMergeTree で tx_uuid 単位の最新行を保持
--   (CDC は再送で重複しうるため、id バージョンで上書きする)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.transactions
(
    id                Int64,
    tx_uuid           String,
    user_id           Int32,
    amount            Float64,
    merchant_category String,
    country           String,
    device            String,
    is_fraud          UInt8,
    created_at        DateTime64(3),
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(id)
ORDER BY tx_uuid;

-- ---------------------------------------------------------------------------
-- Sink -> DWH を繋ぐ Materialized View
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.transactions_mv TO dwh.transactions AS
SELECT
    id,
    tx_uuid,
    user_id,
    amount,
    merchant_category,
    country,
    device,
    is_fraud,
    parseDateTime64BestEffort(created_at) AS created_at
FROM dwh.transactions_queue;
