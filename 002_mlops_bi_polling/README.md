# 002_mlops_bi_polling — ストリーミング → BI 可視化パイプライン

複数クライアントが取引イベントを非同期に投入し、出口=BI(Grafana)で可視化するサンプル。題材は二値分類(fraud 判定)。

構成の要点(定番の「CDC → DWH → バッチ学習 → 推論」パイプラインに対して)
+ 特徴：入口=複数クライアントの非同期投入、出口=BI(Grafana)可視化
+ 共通：中間の CDC → DWH → 前処理 → 学習はそのまま


```
00_clients … ペルソナ別クライアント(非同期にイベント投入 / lat・lon・source 付き)
  │  POST /transactions
  ▼
01_api_ingest … Ingest API(FastAPI :8000)。受けた取引を OLTP へ INSERT
  │
  ▼
02_postgres … PostgreSQL / OLTP。行の変更を WAL に出す
  │  WAL(論理レプリケーション)
  ▼
03_debezium … Debezium / Kafka Connect。WAL を読み Kafka トピックへ publish
  │  topic: oltp.public.transactions
  │  ※ Kafka 自体は基盤サービスで、番号フォルダは持たない
  ▼
04_clickhouse … Kafka Engine が dwh.transactions へ取込(生データ)
  │
  ├─ 学習(バッチ / make train)──────────────
  │   ▼
  │  05_spark … dwh.transactions を前処理 → parquet
  │   ▼
  │  06_trainer … 学習 → Model(/models)
  │  ───────────────────────────────────────
  ▼
07_scorer … 06 の Model で 04 の新着をポーリング推論 → dwh.predictions に書き戻す
  ▼
08_grafana … :3000 で可視化(transactions / predictions)
```

04 から 05→06 で Model を作り、本線へ戻って07 がその Model を使う。

04→07→08 が常時の推論ライン、枝の 05・06 はそこへモデルを供給する区間。


ポイントは、Scorerである。
推論結果を可視化するためには、クエリ可能な形でデータを貯める必要がある。

このためオンデマンド推論 API ではなく、結果を `dwh.predictions` に書き戻す **Scorer** を配置している。


## ディレクトリ構成

| パス | 役割 |
|------|------|
| `00_clients/client.py` | ペルソナ別クライアント(env で地域・レート・不正傾向を変える) |
| `01_api_ingest/` | 入口 FastAPI(`latitude`/`longitude`/`source` を追加) |
| `02_postgres/init/` | OLTP スキーマ(lat/lon/source 付き) |
| `03_debezium/` | Debezium(Postgres source)コネクタ定義 |
| `04_clickhouse/init/` | Kafka Engine(Sink)+ `transactions` + `predictions` + `model_runs` |
| `04_clickhouse/users.d/` | `default` を別コンテナから接続可にする設定 |
| `05_spark/` | DWH を読み前処理 parquet を書く Spark ジョブ。Docker ボリュームに保存。 |
| `06_trainer/` | Docker ボリュームから、parquet を読みモデルを学習(`metadata.json` に指標) |
| `07_scorer/` | 最新モデルで新着をスコア → `dwh.predictions` / `dwh.model_runs` |
| `08_grafana/` | datasource とダッシュボードの provisioning(自動投入) |
| `Makefile` | 一連の操作のショートカット |


```bash
cd 002_mlops_bi_polling

# 1. 基盤 + Scorer + Grafana を起動 (healthcheck 完了まで待機)
make up

# 2. CDC 開始
make connector

# 3. ペルソナ別クライアントを起動(非同期にデータ投入が始まる)
make clients-start

# 4. 少し溜まったら 前処理 + 学習(モデル生成)。Scorer が自動でスコアリングを始める
make train

# 5. ブラウザでダッシュボードを開く(ログイン不要)
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
| `make artifacts` | parquet とモデルの保存状況(更新時刻・サイズ・学習指標) |
| `make ps` | 状態一覧 |
| `make logs` | ログ追尾 |
| `make down` | 停止 |
| `make clean` | ボリュームごと削除 |

## ダッシュボードの内容(Grafana)

`Fraud Streaming Overview` に以下を配置:

- **総取引数 / スコア済み / 不正予測数 / 最新 ROC-AUC**(stat)
- **流入スループット / 分(ペルソナ別)**(時系列・積み上げ)
- **予測 fraud 率 / 分**(時系列)
- **取引マップ**(geomap、色 = fraud 確率。OSM ベースで外部トークン不要)
- **fraud 確率の分布**(ヒストグラム)
- **国別リスク**(平均 fraud 確率テーブル)
- **再学習ごとの ROC-AUC 推移**(時系列)
- **直近の取引 + スコア**(生データテーブル)

## 各ステージの要点

### 入口:複数クライアント(非同期 / ペルソナ別)
- 同一イメージを compose の 4 サービス(`client-jp` / `client-eu` / `client-us` / `client-fraud`)
  として起動。`SOURCE` / `REGION` / `RATE` / `FRAUD_BIAS` を env で変える。
- 各クライアントはポアソン的な間隔(`expovariate`)で投げるので、自然に非同期・バラバラになる。
- 自地域の緯度経度を散らして生成するので、地図に地域ごとの分布・不正ホットスポットが出る。

### 中間:CDC → DWH → 前処理 → 学習
- 定番の CDC パイプライン。Debezium が WAL を unwrap して Kafka へ、ClickHouse Kafka Engine + MV で
  `dwh.transactions` へ。Spark が特徴量化 → parquet、Trainer が RandomForest を学習。
- lat/lon/source は可視化用で、学習特徴量には使わない。

### 出口:Scorer → 書き戻し → Grafana
- Scorer は `/models/model.joblib` を mtime で監視し、更新されたら読み直す。
- 未スコアの取引(`predictions` に無い行)を読み、確率を計算して `dwh.predictions` に insert。
- 再学習を検知したら `metadata.json` の指標を `dwh.model_runs` に記録(ROC-AUC 推移用)。
- Grafana は ClickHouse を直接クエリ。datasource もダッシュボードも provisioning で自動投入。

## トラブルシュート

- **Grafana に「No data」** → まだデータ/モデルが無い。`make clients` → `make train` の順で進め、
  Scorer が `predictions` を埋めるまで待つ(`make verify` で件数確認)。
- **ClickHouse データソースエラー** → プラグイン取得に失敗。`docker compose logs grafana` を確認
  (`GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` の取得にネットワークが要る)。
- **`make connector` が失敗** → connect の healthcheck がまだ。少し待って再実行。
- **`make train` で Spark の JDBC / ClickHouse 認証エラー** → `clickhouse-jdbc` は依存込みの
  `0.7.2-all` を使用、`04_clickhouse/users.d/open-default-network.xml` で `default` ユーザの
  接続元を開放済み(どちらも対処済み。設定変更時は clickhouse コンテナの再作成が必要)。

## 注意(サンプルなので割り切っている点)

- 認証・TLS・スキーマレジストリ未使用。Grafana は匿名 Admin で誰でも閲覧・編集可。
- ラベル `is_fraud` は生成時に付与(本番はラベル後追い)。
- 学習・スコアリングは簡易。モデルレジストリや厳密な特徴量整合性管理は含まない。
