# sample3_mlops_bi_streaming — リアルタイム・スコアリング版

[sample2_mlops_bi_polling](../sample2_mlops_bi_polling) の変形パターン。

出口の推論をSpark Structured Streaming で Kafka を直接購読するリアルタイム処理に置き換えたサンプル。

sample2 との差分
+ 差分：Scorer を、Kafka を直読する常駐ストリーミングジョブに置き換え。
+ 同じ：それ以外は同じ

```
複数クライアント(非同期 / ペルソナ別 / 00_clients)
  ├─ client-jp   (高頻度・低リスク)
  ├─ client-eu   (中頻度)
  ├─ client-us   (中頻度)
  └─ client-fraud(低頻度・高リスク)
  │  各コンテナが独立ループで POST /transactions(lat/lon, source 付き)
  ▼
Ingest API(FastAPI :8000)
  │
  ▼
PostgreSQL / OLTP
  │  WAL(論理レプリケーション)
  ▼
Debezium / Kafka Connect
  │
  ▼
Kafka(topic: oltp.public.transactions)
  │
  ▼
Spark Structured Streaming(stream-scorer / 常駐)   Kafka を直読してリアルタイム推論
  │  from_json → 特徴量 → 最新モデルで推論(foreachBatch)→ dwh.predictions
  ▼
ClickHouse / DWH(predictions + transactions)
  │
  ▼
Grafana(:3000)で可視化(時系列 + 地図)
```

上が本筋(リアルタイム推論)の流れ。これに以下の2本が脇で接続する:

- **生データ取込**: Kafka ──(ClickHouse Kafka Engine)──▶ `dwh.transactions`
  （stream-scorer と同じトピックを別コンシューマグループで購読）
- **学習(バッチ)**: `dwh.transactions` ─▶ Spark 前処理 ─▶ Trainer ─▶ Model（`/models`）
  （stream-scorer はこの Model を mtime 監視で読み込む)

ポイント:同じ Kafka トピックを **ClickHouse Kafka Engine(生データ)** と
**Spark Structured Streaming(推論)** が別々のコンシューマグループで購読する。
推論はポーリングではなくストリームとして流れるため、ほぼリアルタイムに `predictions` が埋まる。

> **リアルタイムなのは「推論」だけ。「学習」はバッチのまま**です。
> stream-scorer は来たイベントを**既存モデル**で即スコアしますが、学習はしません。
> モデルは `make train` を実行したときだけ作られ／更新されます(stream-scorer は mtime 監視で
> 新モデルを自動ロード)。`make train` を再実行しなければ最初のモデルで推論が流れ続けます
> (= 回し続けるだけなら不要。モデルを新鮮に保ちたいときだけ `make train` を再実行)。

## sample2 との対比

| | sample2 | sample3(本サンプル) |
|---|---|---|
| 推論の起点 | ClickHouse を10秒ごとにポーリング | **Kafka を直接 readStream** |
| 実装 | Python 常駐ループ(`07_scorer`) | **Spark Structured Streaming**(`07_stream_scorer`) |
| 鮮度 | ポーリング間隔ぶん遅延 | trigger 間隔(既定5秒)の near-real-time |
| 未スコア検出 | `predictions` との差分クエリ | checkpoint でオフセット管理(再送なし) |

## ディレクトリ構成

| パス | 役割 |
|------|------|
| `00_clients/client.py` | ペルソナ別クライアント(非同期投入、lat/lon/source 付き) |
| `01_api_ingest/` | 入口 FastAPI |
| `02_postgres/init/` | OLTP スキーマ |
| `03_debezium/` | Debezium コネクタ定義 |
| `04_clickhouse/init/` | Kafka Engine(生データ Sink)+ transactions / predictions / model_runs |
| `05_spark/` | 学習用の**バッチ**前処理(DWH → parquet) |
| `06_trainer/` | parquet を読みモデルを学習 |
| `07_stream_scorer/` | **Spark Structured Streaming**:Kafka 直読 → リアルタイム推論 → predictions |
| `08_grafana/` | datasource とダッシュボードの provisioning |
| `Makefile` | 一連の操作のショートカット |

## 動かし方

前提: Docker / Docker Compose v2、curl・bash、**実行時にネット接続**(Spark の
`spark-sql-kafka` コネクタを `--packages` で取得するため)。**ポートが重複するため
sample1 / sample2 とは同時に起動しない**こと。

```bash
cd sample3_mlops_bi_streaming

# 1. 基盤 + stream-scorer + Grafana を起動(healthcheck 完了まで待機)
make up

# 2. CDC 開始
make connector

# 3. ペルソナ別クライアントを起動(非同期にデータ投入)
make clients-start

# 4. 前処理 + 学習でモデルを作る。以降 stream-scorer が自動でスコアし続ける
make train

# 5. ダッシュボード(ログイン不要)
#    http://localhost:3000/d/fraud-overview

# データが増えた頃に定期的に再学習するとROC-AUCが変化する
make train

# 後始末
make down            # ボリュームも消すなら make clean
```

`make all` で 1〜4 を一気に実行できる。

### 補助コマンド

| コマンド | 内容 |
|----------|------|
| `make clients-start` | クライアント群の起動 |
| `make clients-stop` | クライアント群の停止 |
| `make verify` | transactions / predictions / model_runs の件数確認 |
| `make artifacts` | parquet とモデルの保存状況 |
| `make logs` | ログ追尾(`docker compose logs -f stream-scorer` で推論ログだけ追える) |
| `make down` | 停止 |
| `make clean` | ボリュームごと削除 |

## ストリーミングジョブの要点(07_stream_scorer)

- `readStream` で Kafka トピック `oltp.public.transactions`(Debezium が unwrap した flat JSON)を購読。
- `from_json` でパースし、`amount_log` / `hour` / `dow` を**学習時と同じ Spark 関数**で生成。
- `foreachBatch` 内で `/models/model.joblib` を mtime 監視でロード(再学習を自動反映)、
  pandas に変換して `predict_proba`、結果を `clickhouse-connect` で `dwh.predictions` に insert。
- `checkpointLocation` でオフセットを永続化するので、再起動しても二重処理しない。
- `trigger(processingTime="5 seconds")` の**マイクロバッチ**。サブ秒の真のイベント駆動が
  必要なら Flink が候補(本サンプルは既存 Spark 資産を活かす near-real-time 構成)。

## トラブルシュート

- **stream-scorer が起動直後に止まる / Kafka パッケージ取得エラー** → `--packages` の解決に
  ネットワークが必要。`docker compose logs stream-scorer` を確認し、取得できる環境で再実行。
- **predictions が増えない** → 先に `make train` でモデルを作る(モデルが無い間はスコアしない)。
  `docker compose logs -f stream-scorer` で `scored N rows` が出ているか確認。
- **Grafana に「No data」** → データ/モデルがまだ。`make clients` → `make train` の順に。
- **Spark の JDBC / ClickHouse 認証エラー(バッチ前処理)** → [sample1 のトラブルシュート](../sample1_mlops_serving_api/README.md)と同じ。

## 注意(サンプルなので割り切っている点)

- 認証・TLS 未使用。Grafana は匿名 Admin。
- Structured Streaming は既定のマイクロバッチ(near-real-time)。
- 学習はバッチ手動。モデルレジストリや厳密な特徴量整合性管理は含まない。
