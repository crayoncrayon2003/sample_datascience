"""データ準備(prep)ステップ:道路網を osmnx で取得して GraphML 化する.

重い処理なので runtime とは分離し、`make data` で1回だけ実行する。
対象は計算コストを抑えるため大津市・草津市に限定(env PLACES で変更可)。
出力:
  /data/road/road_drive.graphml       … feed が読む道路グラフ(highway/maxspeed/geometry 付き)
  /data/geojson/boundary.geojson      … 地図背景(対象市の境界。任意)
  /data/geojson/road_links.geojson    … 地図背景(実道路リンク線。任意)
既にファイルがあればスキップする。
"""
import os
from pathlib import Path

# 計算コストを抑えるため2市に限定。滋賀全域にしたいなら PLACES を変える。
PLACES = os.environ.get("PLACES", "大津市,草津市").split(",")
PLACES = [p.strip() + ", 滋賀県, 日本" for p in PLACES if p.strip()]
ROAD = "/data/road/road_drive.graphml"
BOUNDARY = "/data/geojson/boundary.geojson"
ROAD_LINKS = "/data/geojson/road_links.geojson"

os.makedirs("/data/road", exist_ok=True)
os.makedirs("/data/geojson", exist_ok=True)

import osmnx as ox  # noqa: E402

ox.settings.log_console = False

G = None
if os.path.exists(ROAD):
    print(f"[SKIP] 既存: {ROAD}(再取得は削除してから)")
else:
    print(f"道路ネットワーク取得中: {PLACES} ...", flush=True)
    G = ox.graph_from_place(PLACES, network_type="drive")
    ox.save_graphml(G, ROAD)
    st = ox.basic_stats(G)
    print(f"[保存] {ROAD}  ノード={st['n']:,} エッジ={st['m']:,} 総延長={st['edge_length_total']/1000:.0f}km")

if os.path.exists(BOUNDARY):
    print(f"[SKIP] 既存: {BOUNDARY}")
else:
    try:
        print("境界ポリゴン取得中 ...", flush=True)
        gdf = ox.geocode_to_gdf(PLACES)
        gdf = gdf.to_crs(4326)
        keep = [c for c in ("display_name", "name", "geometry") if c in gdf.columns]
        gdf = gdf[keep]
        Path(BOUNDARY).write_text(gdf.to_json(), encoding="utf-8")
        print(f"[保存] {BOUNDARY}")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] 境界取得をスキップ: {exc}")

if os.path.exists(ROAD_LINKS):
    print(f"[SKIP] 既存: {ROAD_LINKS}")
else:
    try:
        if G is None:
            G = ox.load_graphml(ROAD)
        edges = ox.graph_to_gdfs(G, nodes=False, fill_edge_geometry=True).reset_index()
        edges["link_id"] = edges.apply(lambda r: f"{r['u']}_{r['v']}_{r['key']}", axis=1)
        for col in ("highway", "maxspeed", "name", "ref"):
            if col in edges.columns:
                edges[col] = edges[col].astype(str)
        keep = [c for c in ("link_id", "highway", "maxspeed", "name", "ref", "length", "geometry") if c in edges.columns]
        Path(ROAD_LINKS).write_text(edges[keep].to_json(), encoding="utf-8")
        print(f"[保存] {ROAD_LINKS}  links={len(edges):,}")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] 道路リンクGeoJSON生成をスキップ: {exc}")

print("prep 完了。`make feeds` でエージェントを起動できます。")
