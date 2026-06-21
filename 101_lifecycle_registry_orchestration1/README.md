# 101_lifecycle_registry_orchestration1 — モデルライフサイクル(MLflow + Dagster)

[003_mlops_bi_streaming](../003_mlops_bi_streaming) のストリーミング基盤(Kafka 直読 →
Spark Structured Streaming → ClickHouse → Grafana)を**そのまま流用**し、
モデルの **管理・配信・再学習(= MLOps の運用ループ)** を作り込んだサンプル。題材は二値分類(fraud 判定)。

`00x` 系(001〜004)が「**推論/可視化パターン**」の学習チェーンなのに対し、
`10x` 系はそこに **MLOps 運用の関心事** を重ねる枝。本サンプルはその第1弾
「**実験管理 + モデルレジストリ + オーケストレーション**」を扱う。
オーケストレータ違いの姉妹サンプルとして **102(Airflow 版)** を予定。

## 003 までとの差分(ここが主題)

| 観点 | 003 まで | 本サンプル(101) |
|---|---|---|
| モデル保存 | `joblib` をボリュームに dump | **MLflow Model Registry** に version 登録 |
| 実験記録 | `metadata.json` 1ファイル | **MLflow Tracking**(全 run の params/metrics/artifact) |
| バージョン | `int(mtime)` | レジストリの **version + alias**(champion / challenger) |
| 昇格 | 無し(上書き) | **champion/challenger ゲート**(ROC-AUC 比較で昇格、ロールバック可) |
| 再学習トリガ | `make train` 手動 | **Dagster** で DAG 化 + 10 分ごとのスケジュール |
| Scorer のモデル取得 | ファイル mtime 監視 | `models:/fraud@champion` を **Registry から解決**、無停止リロード |

> **alias 方式**: MLflow の旧 stage(Staging/Production)は 2.9 以降 **非推奨**。本サンプルは
> 後継の **alias**(`champion`=現役 / `challenger`=次の候補)で世代を管理する。

## アーキテクチャ

本筋(入口 → CDC → Kafka → 推論 → 可視化)のフローは **003 とまったく同じ**。
差分は「モデルの作り方・配り方」だけ。下の1本のフローは 003 の図と同じで、
`★` を付けた箇所だけが 101 で変わる。

003 との差分
+ 差分:Scorer のモデル入手先 → `/models` の mtime 監視 から **MLflow Registry の `@champion`** に変更(図の `★`)
+ 差分:学習が `/models` への dump → **MLflow に version 登録 + champion/challenger 昇格**(後述の脇の流れ)
+ 差分:再学習が `make train` 手動 → **Dagster が 10 分ごとに自動実行**
+ 同じ:入口〜Kafka〜ClickHouse〜Grafana の本筋は 003 と同じ

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
09_stream_scorer … 常駐の Spark Structured Streaming。上のトピックを直読してリアルタイム推論
  │   1. 届いた JSON 文字列を項目にばらす(from_json)
  │   2. 学習時と同じ特徴量を計算する(amount_log / hour / dow …)
  │   3. かたまり(マイクロバッチ)ごとにモデルで推論する(foreachBatch)
  │   4. 結果を書き込む
  │   ★ 003 との唯一の違い = 手順3で使う「モデルの入手先」
  │      003 : 共有ファイル /models/model.joblib を更新時刻(mtime)で見張る
  │      101 : 06_mlflow に「@champion(本番の札)の版をくれ」と聞いて取得し、
  │            札が別 version に貼り替わったら無停止で読み直す
  ▼
04_clickhouse … ClickHouse / DWH。推論結果を dwh.predictions へ
  │              (生データ dwh.transactions は別経路で取込。下記参照)
  ▼
10_grafana … :3000 で可視化(時系列 + 地図)
```

上の縦線が本筋(003 と同一)。これに脇の2本が同じ Kafka トピックから分岐する(番号フォルダで示す):

- **生データ取込**(003 と同じ): `03_debezium` が publish したトピックを `04_clickhouse` の
  Kafka Engine が購読し、`dwh.transactions` へ生データとして取り込む。
- **学習〜配信**(★ ここが 101 の主題): `04_clickhouse`(dwh.transactions)→ `05_spark`(前処理)
  → `07_trainer`(学習 → `06_mlflow` に version 登録 → `@challenger`)→ `07_trainer` の
  `promote.py`(`@champion` 昇格ゲート)。この一連を `08_orchestrator`(Dagster)が 10 分ごとに実行する。
  `09_stream_scorer` は `06_mlflow` から `@champion` を取得して推論に使う。
  （003 の「`06_trainer` → Model(`/models`)を `07_stream_scorer` が mtime 監視」を置き換えた部分)

ポイント:**Dagster 自身は計算を持たない指揮者**。前処理/学習/昇格は既存コンテナのまま、
Dagster は docker socket 経由で `docker compose run` するだけ。資産の依存
(`features → challenger_model → champion_model`)が lineage グラフに可視化される。

## ディレクトリ構成

| パス | 役割 | 003 との差 |
|------|------|------|
| `00_clients/` 〜 `05_spark/` | クライアント〜Kafka〜ClickHouse〜Spark 前処理 | ほぼ流用 |
| `02_postgres/init/00_mlflow_db.sql` | MLflow バックエンド用 DB(`mlflow`)を作成 | ★新規 |
| `06_mlflow/` | MLflow Tracking + Registry サーバ(Postgres backend) | ★新規 |
| `07_trainer/train.py` | 学習 → run 記録 → version 登録 → `@challenger` | ★改造 |
| `07_trainer/promote.py` | 昇格ゲート(champion と比較し `@champion` 付替) | ★新規 |
| `08_orchestrator/` | Dagster(assets + 10 分スケジュール) | ★新規 |
| `09_stream_scorer/` | mtime 監視 → `models:/fraud@champion` 解決に置換 | ★改造 |
| `10_grafana/` | datasource + ダッシュボード(provisioning) | 流用 |

## 使い方

```bash
# 1. 基盤 + stream-scorer + Grafana を起動(healthcheck 完了まで待機)
make up

# 2. CDC 開始
make connector

# 3. ペルソナ別クライアントを起動(非同期にデータ投入)
make clients-start

# 4. 前処理 + 学習でモデルを作る。以降 stream-scorer が自動でスコアし続ける
make train

# 5. 現在の champion / challenger version を表示
make registry

# 後始末
make down            # ボリュームも消すなら make clean
```

- MLflow UI: http://localhost:5000 (experiments / Models > `fraud` の version と alias)
- Dagster UI: http://localhost:3001 (Assets の lineage / Runs / Schedules)
- Grafana: http://localhost:3000/d/fraud-overview

Dagster UI にアクセス、 Assets タブを選択後、次のいずれかを選択する。
+ challenger_model
+ champion_model
+ features

いずれかを選択後、 Lineage タブを選択すると、アセットが確認できる。

Dagster UI にアクセス、 Jobs タブを選択すると、アセットを接続したフローが確認できる。


### champion/challenger を体感する

1. 次のどちらかで新モデルを作る。どちらも train.py を実行する。`fraud` の version が1つ増えて @challenger が付く
   + make train を実行(手動)
   + Dagster の10分ごとのジョブ(definitions.py)
2. promote.py が 新(@challenger)と旧(@champion)の ROC-AUC を比較する。
   勝った時だけ @champion を新 version に付け替える(=本番=stream-scorer が 使うモデルが入れ替わる)。
   負けても version は MLflow に残るが、champion は旧のまま。
3. `09_stream_scorer` は `@champion` の version 変化を検知して無停止で新モデルに切り替える
   `dwh.predictions.model_version` が新しい version 番号に変わる。
4. ロールバックしたい時は、MLflow 上で過去 version に `@champion` を貼り直すだけ。

今、推論に使っているモデルは、MLflow UIで、@championが付いているバージョンのモデル。

## 注意点(ローカルサンプル前提)

- **Dagster が docker socket を使う**:`08_orchestrator` は `/var/run/docker.sock` と
  プロジェクト一式(`./:/project`)をマウントし、ホストの docker に対して `docker compose run`
  する。これは「オーケストレータがコンテナ化された各ステップを駆動する」現実的なパターンだが、
  socket 共有が前提。docker CLI / compose バイナリは **linux/amd64** を取得する
  (arm64 は `08_orchestrator/Dockerfile` の URL を読み替える)。
- **artifact 配信**:MLflow は `--serve-artifacts` でモデル artifact を HTTP プロキシするので、
  Trainer / Scorer はボリューム共有なしに `http://mlflow:5000` 経由で読み書きする。
- 認証・ネットワーク制限・専用ユーザ等は本番で別途必要(ClickHouse の `default` 開放と同様)。
