-- OLTP スキーマ。sample1 との差分: latitude / longitude / source を追加。
CREATE TABLE IF NOT EXISTS transactions (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tx_uuid           TEXT             NOT NULL,
    user_id           INTEGER          NOT NULL,
    amount            DOUBLE PRECISION NOT NULL,
    merchant_category TEXT             NOT NULL,
    country           TEXT             NOT NULL,
    device            TEXT             NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    source            TEXT             NOT NULL,
    is_fraud          SMALLINT         NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- 論理レプリケーション時に変更前イメージも含める
ALTER TABLE transactions REPLICA IDENTITY FULL;
