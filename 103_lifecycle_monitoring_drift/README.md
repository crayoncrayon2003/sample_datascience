# 103_lifecycle_monitoring_drift — モデル監視 & データドリフト検知

ストリーミング推論パイプラインに、**データドリフトとデータ品質の監視**を載せたサンプル。題材は二値分類(fraud 判定)。
「モデルが見た世界(学習時の分布)」を基準に、本番に流れてくる直近データがどれだけズレたか(ドリフト)を
**Evidently** で定期計算し、Grafana で時系列に可視化する。

## このサンプルの要点

| 要素         | 仕組み                                                                                               |
| ------------ | ---------------------------------------------------------------------------------------------------- |
| ドリフト検知 | **Evidently** で「学習時分布(基準 reference)」と「直近データ(current)」を比較                  |
| 監視対象     | 特徴量(`amount` / `merchant_category` / `country` / `device`)のドリフト + データ品質(欠損率) |
| 予測の監視   | 直近の**予測 fraud 率 / 平均確率**(学習時の fraud 率と並べて推移を見る)                        |
| 記録         | `dwh.drift_metrics`(全体サマリ)/ `dwh.drift_by_feature`(列ごと)を時系列で蓄積                    |
| 可視化       | Grafana ダッシュボード`drift-overview`                                                             |
| ドリフト注入 | `make drift` で分布をズラすクライアントを起動(デモ用)                                              |

> ドリフトの基準は **`make train` した時点の学習データ分布**。本番では「ドリフトがしきい値を超えたら
> 再学習をトリガする」運用にする(本サンプルは**検知と可視化まで**を示す)。

## アーキテクチャ

```
00_clients … クライアント(正常分布で投入。make drift で client-drift が分布をズラす)
  │  POST /transactions
  ▼
01_api_ingest … Ingest API(FastAPI :8000)。受けた取引を OLTP へ INSERT
  ▼
02_postgres … PostgreSQL / OLTP。行の変更を WAL に出す
  ▼
03_debezium … Debezium / Kafka Connect。Kafka トピックへ publish
  │  topic: oltp.public.transactions(Kafka は基盤サービス・番号フォルダなし)
  ▼
04_clickhouse … Kafka Engine が dwh.transactions へ取込(生データ)
  │
  ├─ 学習(バッチ / make train)──────────────
  │   ▼
  │  05_spark … 前処理 → parquet(★この分布が「ドリフトの基準 reference」になる)
  │   ▼
  │  06_trainer … 学習 → Model(/models)
  │  ───────────────────────────────────────
  ▼
07_stream_scorer … Kafka を直読してリアルタイム推論 → dwh.predictions
  ▼
09_grafana … :3000 ドリフト監視ダッシュボード(drift-overview)

[監視ループ] 08_monitor(常駐)
  ・基準  = 05_spark が出した学習用 parquet(モデルが見た分布)
  ・直近  = dwh.transactions の直近ウィンドウ(既定 2000 件)
  → Evidently で特徴量ドリフト/データ品質を算出、予測の要約も付けて
     dwh.drift_metrics / dwh.drift_by_feature へ定期 insert(既定 30 秒ごと)
  → 09_grafana がそれを時系列で表示
```

要点は **08_monitor**:推論や学習とは独立した常駐プロセスで、DWH を読んで「分布のズレ」を測り続ける。
基準は固定(学習時 parquet)、current は流れ続けるので、データが変わると drift が上がる。

## ディレクトリ構成

| パス                                   | 役割                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------- |
| `00_clients/client.py`               | ペルソナ別クライアント。`DRIFT_AFTER` で途中から分布をズラせる(ドリフト注入)              |
| `01_api_ingest/` 〜 `03_debezium/` | 入口 FastAPI / OLTP / CDC(Debezium → Kafka)                                                |
| `04_clickhouse/init/`                | Kafka Engine +`transactions` / `predictions` / `drift_metrics` / `drift_by_feature` |
| `05_spark/` `06_trainer/`          | バッチ前処理(parquet=基準)+ fraud 二値分類の学習                                            |
| `07_stream_scorer/`                  | Spark Structured Streaming:Kafka 直読 → リアルタイム推論 → predictions                    |
| `08_monitor/monitor.py`              | **Evidently でドリフト/品質を定期計算 → `dwh.drift_metrics` へ記録**               |
| `09_grafana/`                        | ドリフト監視ダッシュボード(provisioning で自動投入)                                         |
| `Makefile`                           | 一連の操作のショートカット                                                                  |

## 使い方

前提: Docker / Docker Compose v2、curl・bash、**実行時にネット接続**(Spark の Kafka コネクタを
`--packages` で取得するため)。**ポートが重複するため、他のサンプルとは同時に起動しない**こと。

```bash
cd 103_lifecycle_monitoring_drift

# 1. 基盤 + stream-scorer + monitor + Grafana を起動
make up

# 2. CDC 開始
make connector

# 3. 正常分布のクライアントを起動(データ投入)
make clients

# 4. 前処理 + 学習。★この時点の分布が「ドリフトの基準」になる
make train

# 5. ドリフト監視ダッシュボードを開く(ログイン不要)
#    http://localhost:3000/d/drift-overview

# 6. ドリフトを注入して、ダッシュボードのドリフトが上がるのを見る
make drift

# 後始末
make down            # ボリュームも消すなら make clean
```

`make verify` で `drift_metrics` の件数と最新のドリフト指標を確認できる。

### ドリフトを体感する

1. `make clients` → `make train` で「正常時の分布」を基準として焼き込む。
2. しばらくは `drift_share` がほぼ 0(基準と current が同じ分布)。
3. `make drift` で `client-drift` を投入。これは時間とともに **金額を高額側へ・国を高リスク側へ・
   カテゴリを electronics/jewelry へ・不正率を上げ**ていく(5 分かけてランプ)。
4. 直近ウィンドウが drift 側に寄ると、Grafana の **drift share / dataset drift / 特徴量ごとのドリフトスコア**が
   上がり、**予測 fraud 率**も基準から離れていくのが見える。
5. 本番ならここで再学習をトリガする。基準を更新したいときは `make train` を再実行する
   (新しい parquet が新しい基準になる)。

## ダッシュボードの内容(`drift-overview`)

- **最新ドリフト割合 / 全体ドリフト判定 / ドリフト特徴量数**(stat)
- **欠損率(データ品質)/ 予測 fraud 率 / 評価ウィンドウ件数**(stat)
- **ドリフト割合の推移**(時系列)
- **予測 fraud 率 vs 学習時の基準**(時系列)
- **特徴量ごとのドリフトスコア**(時系列・列別)
- **平均 fraud 確率の推移**(時系列)

## 注意点(ローカルサンプル前提)

- **基準の取り方**:本サンプルは「学習用 parquet」を基準にする(= モデルが学習した分布からの乖離)。
  運用によっては「直近の安定期間」を基準にするなど選択肢がある。
- **ドリフト ≠ 性能劣化**:ドリフトは入力分布のズレを示すだけ。実際の精度劣化はラベルが届いてから
  測る(本サンプルはラベル即時付与なので簡易)。検知はあくまで「再学習を検討する合図」。
- **検知止まり**:しきい値超えで自動再学習まではしない(検知と可視化が主題)。再学習は `make train`(手動)。
- 認証・TLS 未使用。Grafana は匿名 Admin。ドリフト計算は直近ウィンドウの単純比較(厳密な統計設計は簡略)。
