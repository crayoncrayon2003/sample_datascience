# 102_lifecycle_registry_orchestration2 — モデルライフサイクル(MLflow + Airflow)

取引イベントをリアルタイムに推論しながら、モデルを **MLflow で版管理し、Airflow で定期的に
再学習・昇格**する、MLOps 運用ループのサンプル。題材は二値分類(fraud 判定)。
入口の複数クライアント → CDC → Kafka → ストリーミング推論 → BI 可視化 の一連の上に、
**モデルレジストリ + champion/challenger 昇格 + オーケストレーション**を載せている。

## このサンプルの要点

| 要素 | 仕組み |
|---|---|
| モデル保存 | **MLflow Model Registry** に version 登録 |
| 実験記録 | **MLflow Tracking**(全 run の params / metrics / artifact) |
| バージョン管理 | **version + alias**(`champion`=本番 / `challenger`=次の候補) |
| 昇格 | **champion/challenger ゲート**(ROC-AUC 比較で昇格。ロールバックも alias 貼替だけ) |
| 再学習 | **Airflow** が DAG 化 + 10 分ごとのスケジュール実行 |
| 推論のモデル取得 | `models:/fraud@champion` を **Registry から解決**し、無停止リロード |

> **alias 方式**: MLflow の旧 stage(Staging/Production)は 2.9 以降 **非推奨**。本サンプルは
> 後継の **alias**(`champion`=現役 / `challenger`=次の候補)で世代を管理する。

## アーキテクチャ

入口から可視化まで1本のストリーミングフロー。番号フォルダ順に上から下へ流れる。
モデルは MLflow Registry に登録し、推論側はそこから `@champion`(本番の札)を取得して使う。

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
09_stream_scorer … 常駐の Spark Structured Streaming。Kafka を直読してリアルタイム推論
  │   1. 届いた JSON 文字列を項目にばらす(from_json)
  │   2. 学習時と同じ特徴量を計算する(amount_log / hour / dow …)
  │   3. かたまり(マイクロバッチ)ごとにモデルで推論する(foreachBatch)
  │   4. 結果を dwh.predictions に書き込む
  │   ・推論に使うモデルは 06_mlflow の Registry から @champion を解決して取得し、
  │     champion が別 version に貼り替わったら無停止で読み直す
  ▼
04_clickhouse … ClickHouse / DWH。推論結果を dwh.predictions へ
  │              (生データ dwh.transactions は別経路で取込。下記参照)
  ▼
10_grafana … :3000 で可視化(時系列 + 地図)
```

上の縦線が本筋。これに脇の2本が同じ Kafka トピックから分岐する(番号フォルダで示す):

- **生データ取込**: `03_debezium` が publish したトピックを `04_clickhouse` の
  Kafka Engine が購読し、`dwh.transactions` へ生データとして取り込む。
- **学習〜配信**: `04_clickhouse`(dwh.transactions)→ `05_spark`(前処理)
  → `07_trainer`(学習 → `06_mlflow` に version 登録 → `@challenger`)→ `07_trainer` の
  `promote.py`(`@champion` 昇格ゲート)。この一連を `08_orchestrator`(Airflow)が 10 分ごとに実行する。
  `09_stream_scorer` は `06_mlflow` から `@champion` を取得して推論に使う。

ポイント:**Airflow 自身は計算を持たない指揮者**。前処理/学習/昇格は既存コンテナのまま、
Airflow は docker socket 経由で `docker compose run --no-deps` するだけ。タスクの依存
(`features → challenger_model → champion_model`)が DAG の Graph ビューに可視化される。

## ディレクトリ構成

| パス | 役割 |
|------|------|
| `00_clients/` 〜 `05_spark/` | クライアント〜Kafka〜ClickHouse〜Spark 前処理(特徴量 parquet 生成) |
| `02_postgres/init/00_mlflow_db.sql` | MLflow バックエンド用 DB(`mlflow`)を作成 |
| `06_mlflow/` | MLflow Tracking + Registry サーバ(Postgres backend / `--serve-artifacts`) |
| `07_trainer/train.py` | 学習 → run 記録 → version 登録 → `@challenger` |
| `07_trainer/promote.py` | 昇格ゲート(champion と ROC-AUC 比較し `@champion` 付替) |
| `08_orchestrator/` | Airflow(`dags/retrain.py` の DAG + 10 分スケジュール) |
| `09_stream_scorer/` | `models:/fraud@champion` を Registry から解決してリアルタイム推論 |
| `10_grafana/` | datasource + ダッシュボード(provisioning) |

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
- Airflow UI: http://localhost:8080 (DAG `retrain_pipeline` / Runs / Schedule)。ユーザは `admin`、
  パスワードは `make airflow-pass` で表示(standalone が自動生成)。
- Grafana: http://localhost:3000/d/fraud-overview

Airflow UI にアクセスし、DAG 一覧から `retrain_pipeline` を開く。
- **Graph** ビュー:タスク `features → challenger_model → champion_model` の依存が見える。
- **Grid** ビュー:10 分ごとの実行履歴(各タスクの成否)が見える。
- 右上の ▶(Trigger DAG)で手動実行もできる。


### champion/challenger を体感する

1. 次のどちらかで新モデルを作る。どちらも train.py を実行する。`fraud` の version が1つ増えて @challenger が付く
   + make train を実行(手動)
   + Airflow の10分ごとの DAG `retrain_pipeline`(dags/retrain.py)
2. promote.py が 新(@challenger)と旧(@champion)の ROC-AUC を比較する。
   勝った時だけ @champion を新 version に付け替える(=本番=stream-scorer が 使うモデルが入れ替わる)。
   負けても version は MLflow に残るが、champion は旧のまま。
3. `09_stream_scorer` は `@champion` の version 変化を検知して無停止で新モデルに切り替える
   `dwh.predictions.model_version` が新しい version 番号に変わる。
4. ロールバックしたい時は、MLflow 上で過去 version に `@champion` を貼り直すだけ。

今、推論に使っているモデルは、MLflow UIで、@championが付いているバージョンのモデル。

## 注意点(ローカルサンプル前提)

- **Airflow が docker socket を使う**:`08_orchestrator` は `/var/run/docker.sock` と
  プロジェクト一式(`./:/project`)をマウントし、DAG の各タスク(BashOperator)が
  ホストの docker に対して `docker compose run --no-deps` する。これは「オーケストレータが
  コンテナ化された各ステップを駆動する」現実的なパターンだが、socket 共有が前提。
  docker CLI / compose バイナリは **linux/amd64** を取得する(arm64 は `08_orchestrator/Dockerfile` の URL を読み替える)。
- **Airflow は standalone**:SQLite + SequentialExecutor の簡易構成(webserver + scheduler を一括起動)。
  サンプル用で、本番は LocalExecutor/CeleryExecutor + Postgres メタDB にする。
  `--no-deps` 付きなので、10 分ごとのスケジュール実行が稼働中の基盤を作り直して止めることはない。
- **artifact 配信**:MLflow は `--serve-artifacts` でモデル artifact を HTTP プロキシするので、
  Trainer / Scorer はボリューム共有なしに `http://mlflow:5000` 経由で読み書きする。
- 認証・ネットワーク制限・専用ユーザ等は本番で別途必要(ClickHouse の `default` 開放と同様)。
