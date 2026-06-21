# 道路網エージェント + 道路リンク渋滞予測 + 地図

実在の道路網(OpenStreetMap / osmnx)の上を走る車両エージェントにしたサンプル。

交差点でランダムに次の道を選ぶ agent-based シミュレーションで、地図は道路リンクごとにローリング平均速度で着色。

計算コストを抑えるため対象は**大津市・草津市**に限定。

```
prep/ … osmnx で大津市・草津市の道路グラフ(graphml)を取得
  │  
  ▼
00_feeds(feed.py)… 道路網エージェント。実時間で走行し実時刻 hour で速度を決める
  │  POST /positions/batch(lat/lon, speed, link_id, road_type, maxspeed, sim_hour=実時刻)
  ▼
01_api_ingest … Ingest API。受けた位置を OLTP へ INSERT
  ▼
02_postgres … PostgreSQL / OLTP。行の変更を WAL に出す
  ▼
03_debezium … Debezium / Kafka Connect。Kafka トピックへ publish
  │  topic: oltp.public.vehicle_positions(Kafka は基盤サービス・番号フォルダなし)
  ▼
04_clickhouse … Kafka Engine が dwh.vehicle_positions へ取込(生データ=学習にも使う)
  │
  ├─ 学習(バッチ / make train)──────────────
  │   ▼
  │  05_spark … vehicle_positions を集計・ラベル付け → parquet
  │   ▼
  │  06_trainer … 学習 → Model(/models)
  │  ───────────────────────────────────────
  ▼
07_stream_scorer … Kafka を直読してリンク渋滞をリアルタイム予測。06 の Model を使う
  │  特徴量(road_type/maxspeed/sim_hour/sim_dow)→ 3クラス分類 → dwh.congestion_pred に書込
  ▼
08_grafana … :3000 地図。道路リンクを矢印+ローリング平均速度で色分け、車両を点で重畳
  │           矢印の位置・向きは 09_linkgeo が算出した dwh.link_geo を使う
```

本線(`04 → 07 → 08`)が常時のリアルタイム推論、`├─` の枝が `make train` のバッチ学習。
**地図・推論・学習がすべて同じ `dwh.vehicle_positions`(実時間のライブ)を使う**(偽時間・合成データは無し)。

**1本フローのトレードオフ(精度が上がりにくい理由)**:
学習に使えるのは「実際に車が走った道路種別 × その時に経過した時間帯」だけ。
- ランダムウォークは交通量に比例するので **residential に偏り**、レアな道(motorway_link)はほぼ溜まらない。
- 時間帯も **実時間ぶんしか溜まらない**(全 24 時間を揃えるには丸1日走らせる必要がある)。

→ 学習データは**少なく不均衡**で `(road_type × hour)` の格子に穴が空き、**予測精度は上がりにくい**。
本サンプルはこれを**許容する**(偽の時間で水増ししない、という選択)。

> 現実の MLOps も同じ1本のソース(=実際に流れたログ)で学習する。違いは本番が**数ヶ月**ためて
> 空白を自然に埋める点。本サンプルは短時間しか回さないので精度が出ない、という前提で見る。
> 速度・ラベルの決め方は共有 `congestion.py` に1か所で集約している。

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
- **速度の決め方**: 共有 `congestion.py` が `速度 = maxspeed × 時間帯係数 × 道路種別係数 × link_factor × ノイズ`
  で生成(ラッシュ 7-9/17-19時、幹線ほど混む)。ライブ feed がこの式で速度を出し、その実測がそのまま
  学習データになる。ラッシュ時も赤一色に寄りすぎないよう、道路種別・リンク固有係数・ノイズで
  赤/黄/緑が混ざる強さにしている。**時刻の偽加速(TIME_SCALE)は廃止**=時刻は実時刻のみ。
  全時間帯を人工的に埋めることはしない(その結果が上記のトレードオフ)。

## ディレクトリ構成

| パス | 役割 |
|------|------|
| `prep/` | osmnx で道路 graphml、境界 GeoJSON、道路リンク GeoJSON を取得(make data。大津市+草津市) |
| `00_feeds/graphml_road.py` | osmnx GraphML を stdlib で読む(有向 link_id/road_type/maxspeed、networkx 不要) |
| `00_feeds/congestion.py` | **共有の渋滞モデル**(速度・ラベルの決め方)。feed が使う |
| `00_feeds/feed.py` | 道路網エージェントが実時間で走行し位置を batch POST(地図・推論・学習の共通ソース) |
| `01_api_ingest/` | 入口 FastAPI(`/positions`、`/positions/batch`) |
| `02_postgres/` `03_debezium/` `04_clickhouse/` | OLTP / CDC / DWH(`vehicle_positions` / `congestion_pred` ほか) |
| `05_spark/` `06_trainer/` | バッチ前処理(`vehicle_positions` をラベル付け)+ 渋滞3クラス分類の学習 |
| `07_stream_scorer/` | Spark Structured Streaming:Kafka 直読 → リンク渋滞をリアルタイム予測 |
| `08_grafana/` | 地図(geomap)+ 時系列ダッシュボード、境界/道路リンク GeoJSON |
| `09_linkgeo/` | graphml からリンクの中点・進行方位を計算し `dwh.link_geo` へ投入(地図の矢印用) |
| `Makefile` | 一連の操作のショートカット |

## 動かし方

前提: Docker / Docker Compose v2、curl・bash、**実行時にネット接続**。
**ポートが重複するため、他のサンプルとは同時に起動しない**こと。

```bash
cd 004_mlops_roadmesh_streaming

# 1. 道路データを用意
make data        # 実データ:osmnx で大津市+草津市(数分)

# 2. 基盤 + stream-scorer + Grafana を起動
make up

# 3. CDC 開始
make connector

# 3.5 地図の矢印用に、リンクの中点・進行方位を投入(1回。make data 済みが前提)
make linkgeo

# 4. 道路網エージェントを起動(実時間で走行、車両位置を投入)。しばらく走らせてデータを溜める
make feeds

# 5. 溜まったライブ実データで学習(Spark集計 → Trainer)。以降 stream-scorer が新モデルで予測
#    ※ 実時間ぶんしか溜まらないので、長く走らせるほど学習データが増える(それでも少なめ)
make train        # 内訳: spark(vehicle_positions 集計) → trainer

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
- **Spark の JDBC / ClickHouse 認証エラー** → `clickhouse-jdbc` は `0.7.2-all` を使用、
  `04_clickhouse/users.d/open-default-network.xml` で `default` ユーザの接続元を開放済み。

## 注意(サンプルなので割り切っている点)

- 渋滞は速度から定義した簡易合成。実際の渋滞ラベル(プローブ/センサ)とは異なる。
- エージェントはランダムウォーク(OD・信号・車線なし)。地図のリンク着色は観測ローリング平均、
  モデルは道路属性レベルの予測(リンク固有の癖は学習しない近似)。
- 実データの取得・描画は範囲が広いほど重い。学習はバッチ手動、Structured Streaming はマイクロバッチ。
