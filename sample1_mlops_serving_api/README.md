# sample1_mlops_serving_api — ストリーミング ML パイプライン

取引データ(transaction)を題材に、**CDC でデータを集約し、バッチで学習して、API で推論する**サンプルです。

題材は二値分類(fraud 判定)。

+ 入口の Client(00_client)は、データを流し込む
+ 出口の Client(08_client)は、学習済みモデルに予測を要求する


```
Client(入口 / 00_client)
  │  POST /transactions
  ▼
Ingest API (FastAPI :8000)
  │  INSERT
  ▼
PostgreSQL / OLTP        ── wal_level=logical
  │  WAL (logical decoding)
  ▼
Debezium / Kafka Connect (:8083)   ── ExtractNewRecordState で flat 化
  │
  ▼
Kafka (:9092)   topic: oltp.public.transactions
  │
  ▼
ClickHouse Sink  ── Kafka Engine テーブル (transactions_queue)
  │  Materialized View
  ▼
ClickHouse / DWH (:8123)  ── ReplacingMergeTree (transactions)
  │  JDBC read
  ▼
Spark / 前処理   ── 重複排除・特徴量生成 → parquet (/data/features)
  │
  ▼
Python Trainer / 学習  ── sklearn Pipeline (OneHot + RandomForest)
  │
  ▼
Model (/models/model.joblib)
  │
  ▼
Serving API (FastAPI :8001)
  │  ▲  POST /predict
  ▼  │  fraud_probability
Client(出口 / 08_client)
```


## ディレクトリ構成

フォルダ名の数字は、パイプラインの流れの順番です

| パス | 役割 |
|------|------|
| `00_client/client_seed.py` | Client(入口)。取引イベントを流し込むシミュレータ |
| `01_api_ingest/` | Client の受け口。OLTP に書き込む FastAPI |
| `02_postgres/init/` | OLTP スキーマ。`wal_level=logical` は compose 側で設定 |
| `03_debezium/register-postgres.json` | Debezium(Postgres source)コネクタ定義 |
| `04_clickhouse/init/` | Kafka Engine(Sink)+ MergeTree(DWH)+ MV |
| `04_clickhouse/users.d/` | `default` ユーザを別コンテナ(Spark)からも接続可にする設定 |
| `05_spark/` | DWH を読み前処理 parquet を書く Spark ジョブ。parquetは、Docker ボリュームに保存 |
| `06_trainer/` | Docker ボリュームから、parquet を読みモデルを学習して保存 |
| `07_api_serving/` | モデルをロードして予測を返す FastAPI |
| `08_client/client_predict.py` | Client(出口)。Serving API に予測を要求するクライアント |
| `Makefile` | 一連の操作のショートカット |

## 動かし方

前提: Docker / Docker Compose v2、ホストに Python3(client スクリプト用)・curl・bash。
操作はすべて `make` で完結します(生の docker / curl を直接叩く必要はありません)。

```bash
cd sample1_mlops_serving_api

# 1. 基盤を起動 (build -> up -> healthcheck 完了まで自動で待機)
make up

# 2. Debezium コネクタを登録 → CDC 開始 (内部で起動待ちも行う)
make connector

# 3. Client がイベントを投入
## デフォルト 2000 件
make seed              

## 件数指定
N=5000 make seed 

# 4. OLTP → Kafka → ClickHouse まで流れたか確認
make verify

# 5. 前処理(Spark)→ 学習(Trainer) を実行しモデルを保存
make train

# 6. Client(出口) が Serving API に予測を要求
## デフォルト 10 件
make predict

## 件数指定
N=10 make predict


# 後始末
make down              # ボリュームも消すなら make clean
```

### 一括 / 補助コマンド

| コマンド | 内容 |
|----------|------|
| `make all` | `up` から `predict` まで一気通貫(CDC 伝播待ちも込み) |
| `make artifacts` | parquet とモデルの保存状況(更新時刻・サイズ・学習指標)を表示 |
| `make ps` | サービスの状態一覧(`docker compose ps` のラッパ) |
| `make wait` | 各サービスの healthcheck が通るまでブロック |
| `make build` | イメージのビルドのみ |
| `make start` | 既存コンテナの起動 |
| `make stop` | 既存コンテナの停止 |
| `make logs` | ログを追尾表示 |
| `make down` | 停止 |
| `make clean` | ボリュームごと削除 |

### 3ターミナルでストリーミングのように動かす

基盤を起動しコネクタを登録した状態で、`seed`(投入) / `train`(学習) / `predict`(推論)を
別々のターミナルで回すと、データが流れ続ける様子を体感できます。

#### Step1：準備

1つのターミナルで一度だけ実行する

```bash
cd sample1_mlops_serving_api
make up          # 基盤を起動 (healthcheck 完了まで待機)
make connector   # CDC 開始
```

#### Step2：ストリーミング実行

ターミナルを3つ開いて、それぞれで実行する。

```bash
# ── ターミナル1 : データ投入(入口の Client) ───────────────────
# 2000件ずつ繰り返し投入し続ける
while true; do N=2000 make seed; sleep 5; done

# ── ターミナル2 : 学習(Spark 前処理 → Trainer) ──────────────────
# 15秒ごとに、増えたデータで再学習してモデルを更新し続ける
while true; do make train; sleep 15; done

# ── ターミナル3 : 推論(出口の Client) ─────────────────────────
# 5秒ごとに Serving API へ予測を要求し続ける
while true; do N=10 make predict; sleep 5; done
```

parquetとモデルは、Docker ボリュームに保存している。4つ目のターミナルで、更新の様子を確認する。

データが増えるたび(ターミナル1)→ parquet が書き換わり(ターミナル2の前処理)→ モデルが更新される(同・学習)ので、`n_train` の数字が増えていくのが確認できます。

```bash
watch -n 15 make artifacts
```

## 各ステージの要点

### Ingest API → PostgreSQL
- `POST /transactions` で受けた取引を `transactions` テーブルに INSERT する。


### PostgreSQL → Debezium → Kafka (CDC)
- compose で `wal_level=logical` を有効化、テーブルは `REPLICA IDENTITY FULL`。
- Debezium が `pgoutput` で WAL を読み、`oltp.public.transactions` トピックへ publish。
- `ExtractNewRecordState`(unwrap)SMT で Debezium のエンベロープを外し、 **行そのものの flat JSON** にしているので下流(ClickHouse)が扱いやすい。

### Kafka → ClickHouse (Sink + DWH)
- `dwh.transactions_queue`(**Kafka Engine** テーブル)が Sink。`JSONEachRow` で読む。
- Materialized View `dwh.transactions_mv` が、本体の `dwh.transactions`(**ReplacingMergeTree**)へ流し込む。CDC の再送による重複は `id` をバージョンにした ReplacingMergeTree と、Spark 側の重複排除で吸収する。

### ClickHouse → Spark (前処理)
- `dwh.transactions` を JDBC で読み、tx_uuid 単位で最新行へ重複排除。
- `amount_log` / `hour` / `dow` などの特徴量を作り、parquet を共有ボリューム`/data/features` に出力。

### Spark → Trainer → Model
- parquet を読み、カテゴリ変数は OneHotEncoder、数値はそのままで RandomForest を学習。
- 前処理込みの sklearn Pipeline を `/models/model.joblib` に保存(`metadata.json` に指標)。

### Model → Serving API
- 起動後の初回 `/predict` でモデルを遅延ロード(学習前は 503)。
- 入力特徴量を渡すと fraud 確率を返す。`amount_log` は `amount` から API 内で計算。

## トラブルシュート
- **`make connector` が失敗** → connect の healthcheck がまだ。`docker compose logs connect` を確認し少し待つ。
- **ClickHouse に行が来ない** → コネクタ状態 `curl localhost:8083/connectors/oltp-postgres-connector/status` が RUNNING か、
  トピックが出来ているか `docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list` を確認。
- **`make train` で Spark の JDBC エラー** → clickhouse が healthy か、`CLICKHOUSE_URL` を確認。
- **Spark から ClickHouse が `Authentication failed (Code: 516)`** → 公式イメージは `default`
  ユーザを localhost 限定にするため。`04_clickhouse/users.d/open-default-network.xml` で
  全ネットワークを許可している(この設定が効くよう clickhouse コンテナの再作成が必要)。
- **ClickHouse JDBC で `NoClassDefFoundError: ...ClickHouseClient`** → `clickhouse-jdbc` の
  `0.6.5-all` は依存欠落の壊れた jar。`0.7.2-all` を使うこと(`05_spark/Dockerfile`)。
- **Spark イメージが `bitnami/spark not found`** → Bitnami の無料配布終了のため。公式
  `spark:3.5.8-python3` を使用(PySpark なので python3 タグ)。
- **`make predict` が 503** → 先に `make train` でモデルを作る。

## 注意 (サンプルなので割り切っている点)

- 認証・TLS・スキーマレジストリは未使用。ローカル検証用の最小構成。
- ラベル `is_fraud` は生成時に付与している(本番はラベルが後追いになる想定)。
- 学習はバッチ手動実行。再学習スケジューリングやモデルレジストリ等は含まない。
