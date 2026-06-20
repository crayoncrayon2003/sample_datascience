# sample4_mlops_roadmesh_streaming — 道路網エージェント + 道路リンク渋滞予測 + 地図

[sample3_mlops_bi_streaming](../sample3_mlops_bi_streaming) のストリーミング基盤(Kafka 直読 →
Spark Structured Streaming → ClickHouse → Grafana)を流用し、入口を**実在の道路網
(OpenStreetMap / osmnx)の上を走る車両エージェント**にしたサンプル。**交差点でランダムに次の道を
選ぶ agent-based シミュレーション**で、地図は**道路リンクごとにローリング平均速度で着色**(観測数しきい値つき)。
計算コストを抑えるため対象は**大津市・草津市**に限定(変更可)。

車両エージェントの役割を **2系統に分離**しているのが要点です(以前は1つの feed が兼ねていて
「実時間の可視化」と「偽の高速時刻での学習データ生成」が混ざっていた)。

```
[prep] osmnx で大津市・草津市の道路グラフ(graphml)を取得   ← make data(1回)
        ▼

=== ① ライブ系統(可視化・実時間)===================================
道路網エージェント feeds(00_feeds/feed.py)
  実時間で走行・実時刻の hour で速度を決める(既定TZ=Asia/Tokyo、偽の時刻加速なし)
  │  POST /positions/batch(lat/lon, speed, link_id, road_type, maxspeed, sim_hour=実時刻)
  ▼
Ingest API → PostgreSQL → Debezium → Kafka(oltp.public.vehicle_positions)
  ├──────────────► ClickHouse Kafka Engine → dwh.vehicle_positions(生データ)
  ▼
Spark Structured Streaming(stream-scorer) … Kafka 直読でリンク渋滞をリアルタイム予測
  │  特徴量(road_type/maxspeed/sim_hour/sim_dow)→ 3クラス分類 → dwh.congestion_pred
  ▼
Grafana(:3000)地図 … 道路リンクを矢印+ローリング平均速度で色分け、車両を点で重畳

=== ② 学習系統(バッチ・全時間帯を生成)=============================
trainsim(00_feeds/trainsim.py / make traindata)
  動く車も偽時計も無し。全時間帯×道路種別を直接スイープし合成サンプルを生成
  │  共有の渋滞モデル congestion.py(ライブと同一)で速度→ラベル
  ▼  直接バルク投入
ClickHouse dwh.vehicle_history(road_type/maxspeed/sim_hour/sim_dow/speed)
  ▼
Spark 前処理(集計)→ Trainer 学習 → Model(/models)
```

**2系統の対比**:
- ① ライブ = 実時間・実時刻・滑らかな移動。地図の青い点と観測渋滞、リアルタイム推論に使う。
- ② 学習 = 全時間帯を網羅した合成データ。動く車は不要。`dwh.vehicle_history` を経由して学習。

リアルタイムなのは推論だけ、学習はバッチ(`make traindata` → `make train`)。
共有の `congestion.py` で「速度の決め方」を1か所に統一し、①と②の整合を保つ。

## このサンプルの特徴

| 観点 | 内容 |
|---|---|
| 地理 | **実道路網(OSM, 大津市+草津市)** を osmnx で取得 |
| 動き | **道路グラフをランダムウォーク**(OSMnx の有向リンクを尊重し、交差点で次リンクを選択、実距離ベースで滑らかに走行) |
| 表示単位 | **道路リンク**ごとに**進行方向の矢印**(リンク中点に固定)を置き、**直近のローリング平均速度**で着色(**観測数しきい値**未満は色を出さない)。矢印の向き=リンクの進行方向(有向リンク)、位置は時間で動かない |

## 渋滞の定義と予測(粗さ・自己充足を避ける工夫)

- **地図の色 = 観測のローリング平均**: 散らばった瞬間の点ではなく、**リンクごとに直近5分の平均速度**を
  計算し、`<12=渋滞(赤) / <24=やや(黄) / それ以外=空き(緑)`。**観測数 n≥10** のリンクだけ着色する
  (少数の点で塗らない)。これが「1km四方を1色」だった粗さへの対処。
- **矢印の出し方(必要な間だけ表示)**: 色は直近5分の平均で安定させつつ、**表示するかは「直近1分に通行が
  あったか」で判定**する(refId B の `last_seen > now() - INTERVAL 60 SECOND`)。これで通行が途切れた
  リンクの矢印は約1分で消え、一方的に増え続けない。常時すべてのリンクを出したい場合は、この recency
  条件を外して `dwh.link_geo` を起点に LEFT JOIN し、データ無しをグレーにする方式へ変える。
- **リンクとは**: OSMnx/OSM の道路グラフにおける有向 edge。`source node → target node → edge key`
  で `link_id` を作る。node は交差点だけでなく、行き止まり・道路形状を簡略化した端点も含む。
  渋滞は「区間(辺)をどれだけの速度で流れているか」なので、本来リンク(辺)の性質。地図では
  リンクごとに**矢印を中点へ固定**して置き、**向き=進行方向(有向リンク)・色=渋滞**で表す。
  対面通行の道は `u→v` と `v→u` の2本の別リンクになり、矢印の向きで区別できる。
- **モデルの特徴量** = `road_type(道路種別)/ maxspeed(制限速度)/ sim_hour / sim_dow`。
  **リンク固有の混みやすさ(link_factor)は特徴量に入れない**=モデルから見えない要因として残すので、
  精度は 1.0 に張り付かず(実測 0.93 程度)、自己充足的でない(サンプル1件ずつにラベルを付け、
  平均してから判定しないことで、隠れ要因のばらつきを誤差として残す)。
- **合成の仕掛け**: 共有 `congestion.py` が `速度 = maxspeed × 時間帯係数 × 道路種別係数 × link_factor × ノイズ`
  で ground-truth を生成(ラッシュ 7-9/17-19時、幹線ほど混む)。ライブ feed と学習 trainsim が
  この同じ式を使う。ラッシュ時も赤一色に寄りすぎないよう、道路種別・リンク固有係数・ノイズで
  赤/黄/緑が混ざる強さにしている。**時刻の偽加速(TIME_SCALE)は廃止**:全時間帯のデータは ② の
  trainsim が `sim_hour=0..23` を直接スイープして作る(ライブは実時刻のみ)。

## ディレクトリ構成

| パス | 役割 |
|------|------|
| `prep/` | osmnx で道路 graphml、境界 GeoJSON、道路リンク GeoJSON を取得(make data。大津市+草津市) |
| `00_feeds/graphml_road.py` | osmnx GraphML を stdlib で読む(有向 link_id/road_type/maxspeed、networkx 不要) |
| `00_feeds/congestion.py` | **共有の渋滞モデル**(速度・ラベルの決め方)。feed と trainsim が使う |
| `00_feeds/feed.py` | **① ライブ**:道路網エージェントが実時間で走行し位置を batch POST |
| `00_feeds/trainsim.py` | **② 学習データ生成**:全時間帯×道路種別を合成し `dwh.vehicle_history` へ投入 |
| `01_api_ingest/` | 入口 FastAPI(`/positions`、`/positions/batch`) |
| `02_postgres/` `03_debezium/` `04_clickhouse/` | OLTP / CDC / DWH(`vehicle_positions` / `vehicle_history` ほか) |
| `05_spark/` `06_trainer/` | バッチ前処理(`vehicle_history` をラベル付け)+ 渋滞3クラス分類の学習 |
| `07_stream_scorer/` | Spark Structured Streaming:Kafka 直読 → リンク渋滞をリアルタイム予測 |
| `08_grafana/` | 地図(geomap)+ 時系列ダッシュボード、境界/道路リンク GeoJSON |
| `09_linkgeo/` | graphml からリンクの中点・進行方位を計算し `dwh.link_geo` へ投入(地図の矢印用) |
| `Makefile` | 一連の操作のショートカット |

## 動かし方

前提: Docker / Docker Compose v2、curl・bash、**実行時にネット接続**。
**ポートが重複するため sample1〜4 とは同時に起動しない**こと。

```bash
cd sample4_mlops_roadmesh_streaming

# 1. 道路データを用意
make data        # 実データ:osmnx で大津市+草津市(数分)

# 2. 基盤 + stream-scorer + Grafana を起動
make up

# 3. CDC 開始
make connector

# 3.5 地図の矢印用に、リンクの中点・進行方位を投入(1回。make data 済みが前提)
make linkgeo

# 4. ① ライブ:道路網エージェントを起動(実時間で走行、車両位置の投入)
make feeds

# 5. ② 学習:全時間帯の合成データ生成 → Spark集計 → Trainer 学習(make train が一括実行)
#    以降 stream-scorer が新モデルでライブ予測を続ける
make train        # 内訳: make traindata(dwh.vehicle_history生成) → spark → trainer

# 6. 地図ダッシュボード(ログイン不要)
#    http://localhost:3000/d/roadmesh-overview

# 起動後に再学習する場合は、以下のコマンドだけ再実行
make train

# 後始末
make down            # ボリュームも消すなら make clean
```

### 実データ(大津市・草津市)について

`make data` は `prep/prepare.py` が `osmnx.graph_from_place(['大津市, 滋賀県, 日本', '草津市, 滋賀県, 日本'],
network_type='drive')` で道路グラフを取得し `data/road/road_drive.graphml` に保存、境界を
`08_grafana/geojson/boundary.geojson`、道路リンク線を `08_grafana/geojson/road_links.geojson`
に出力する(範囲は env `PLACES` で変更可)。
feed は `GRAPHML` が指定されていればそのファイルを使い、未指定なら `data/road/road_drive.graphml`
を優先して読む。
すでに graphml を持っているなら `data/road/` に置けば `make data` は不要。

## トラブルシュート

- **feeds が `no graphml found` で終了** → 先に `make data` を実行。
- **矢印(リンク渋滞)が出ない** → 矢印は `dwh.link_geo`(リンク中点・進行方位)と JOIN する。
  `make linkgeo` を実行したか、`dwh.link_geo` に行があるかを確認(`make data` 済みが前提)。
  さらに観測数しきい値(直近5分・n≥10)を満たすリンクだけ表示される。
  矢印の向きが合わない場合は geomap のバージョン差。パネル編集でマーカーの rotation を
  `bearing` フィールド・min0/max360 に設定し直す(本サンプルの既定値)。
- **地図に車両が出ない** → 車両レイヤは直近30秒の位置だけを表示する。`make feeds` の稼働と
  `make verify` の稼働車両数を確認。広い GraphML で表示範囲外に出る場合は `SPAWN_BBOX` を調整する。
  予測レイヤは `make train` 後に stream-scorer が `congestion_pred` を埋めるまで待つ。
- **県境界が出ない** → `make data` で `08_grafana/geojson/boundary.geojson` が生成されているか確認。
- **stream-scorer の Kafka パッケージ取得エラー** → `--packages` 解決にネットが必要。
- **Spark の JDBC / ClickHouse 認証エラー** → [sample1 のトラブルシュート](../sample1_mlops_serving_api/README.md)と同じ。

## 注意(サンプルなので割り切っている点)

- 渋滞は速度から定義した簡易合成。実際の渋滞ラベル(プローブ/センサ)とは異なる。
- エージェントはランダムウォーク(OD・信号・車線なし)。地図のリンク着色は観測ローリング平均、
  モデルは道路属性レベルの予測(リンク固有の癖は学習しない近似)。
- 実データの取得・描画は範囲が広いほど重い。学習はバッチ手動、Structured Streaming はマイクロバッチ。
