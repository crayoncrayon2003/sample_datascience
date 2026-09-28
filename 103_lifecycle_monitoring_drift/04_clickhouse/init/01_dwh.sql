-- =============================================================================
-- ClickHouse : Sink(Kafka Engine)+ DWH(MergeTree)+ 可視化用テーブル
--
--   transactions  … 生データ(地図用 lat/lon, 流入元 source 付き)
--   predictions   … Scorer が書き戻す推論結果
--   model_runs    … 再学習ごとの指標(ROC-AUC 推移の可視化用)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dwh;

-- ---------------------------------------------------------------------------
-- Sink : Kafka からイベントを読み込むキュー
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
    latitude          Float64,
    longitude         Float64,
    source            String,
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
-- DWH 本体(生データ)
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
    latitude          Float64,
    longitude         Float64,
    source            String,
    is_fraud          UInt8,
    created_at        DateTime64(3),
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(id)
ORDER BY tx_uuid;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.transactions_mv TO dwh.transactions AS
SELECT
    id, tx_uuid, user_id, amount, merchant_category, country, device,
    latitude, longitude, source, is_fraud,
    parseDateTime64BestEffort(created_at) AS created_at
FROM dwh.transactions_queue;

-- ---------------------------------------------------------------------------
-- 推論結果(Scorer が書き戻す)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.predictions
(
    tx_uuid           String,
    fraud_probability Float64,
    pred_label        UInt8,
    model_version     String,
    scored_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(scored_at)
ORDER BY tx_uuid;

-- ---------------------------------------------------------------------------
-- 学習の記録(ROC-AUC の推移など)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.model_runs
(
    model_version String,
    trained_at    DateTime64(3),
    n_train       UInt64,
    n_test        UInt64,
    roc_auc       Float64
)
ENGINE = ReplacingMergeTree(trained_at)
ORDER BY model_version;

-- ---------------------------------------------------------------------------
-- ドリフト監視の記録(08_monitor が Evidently で算出して定期 insert)
--   全体: dataset_drift(0/1)/ n_drifted / drift_share / データ品質
--   予測: 予測 fraud 率・平均確率(基準ウィンドウとの差)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.drift_metrics
(
    computed_at      DateTime64(3) DEFAULT now64(3),
    current_rows     UInt32,            -- 評価した直近ウィンドウの件数
    n_features       UInt16,            -- 監視対象の特徴量数
    n_drifted        UInt16,            -- ドリフト判定された特徴量数
    drift_share      Float64,           -- ドリフト特徴量の割合(0..1)
    dataset_drift    UInt8,             -- 全体ドリフト判定(0/1)
    pred_fraud_rate  Float64,           -- 直近ウィンドウの予測 fraud 率
    pred_mean_proba  Float64,           -- 直近ウィンドウの平均 fraud 確率
    ref_fraud_rate   Float64,           -- 基準(学習時)の fraud 率
    missing_share    Float64            -- 欠損セルの割合(データ品質)
)
ENGINE = MergeTree
ORDER BY computed_at;

-- 特徴量ごとのドリフト詳細(列ごとの drift score を時系列で残す)
CREATE TABLE IF NOT EXISTS dwh.drift_by_feature
(
    computed_at  DateTime64(3) DEFAULT now64(3),
    feature      String,
    drift_score  Float64,            -- 検定統計量 / 距離(大きいほどドリフト)
    drifted      UInt8               -- しきい値超え(0/1)
)
ENGINE = MergeTree
ORDER BY (computed_at, feature);
