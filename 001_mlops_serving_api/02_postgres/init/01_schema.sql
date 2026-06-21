-- OLTP スキーマ : Client から届いた取引イベントを保存する
-- amount は Debezium の decimal エンコードを避けるため double precision を使う

CREATE TABLE IF NOT EXISTS transactions (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tx_uuid           TEXT        NOT NULL,
    user_id           INTEGER     NOT NULL,
    amount            DOUBLE PRECISION NOT NULL,
    merchant_category TEXT        NOT NULL,
    country           TEXT        NOT NULL,
    device            TEXT        NOT NULL,
    is_fraud          SMALLINT    NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 論理レプリケーション時に変更前イメージも含める(更新/削除を扱いやすくする)
ALTER TABLE transactions REPLICA IDENTITY FULL;
