"""道路網エージェント・フィード(入口 / ライブ・可視化用).

osmnx の道路グラフ(GraphML)を読み、車両エージェントが交差点で次の道をランダムに選んで
**実時間**で走行する。各エージェントの緯度経度・速度・走行中の道路リンク(link_id)・道路種別・
制限速度・**実時刻**の hour/dow を Ingest API に送る。地図(青い点・観測渋滞・予測)に使う。

速度は共有の渋滞モデル congestion.py(学習データ生成 trainsim.py と同一)で決める。
このフィードは可視化専用で、学習データは作らない(全時間帯の生成は trainsim.py が担当)。
偽の時刻加速(以前の TIME_SCALE)は廃止。残る MOVE_SCALE は地図アニメの再生速度のみ。

env: GRAPHML / INGEST_URL / AGENTS / TICK / MOVE_SCALE / SPAWN_BBOX /
     BATCH_POST / AVOID_UTURN / ADD_REVERSE_EDGES / RANDOM_SEED / SELFTEST
"""
from __future__ import annotations

import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

import congestion
import graphml_road

INGEST_URL = os.environ.get("INGEST_URL", "http://ingest-api:8000")
AGENTS = int(os.environ.get("AGENTS", "200"))
TICK = float(os.environ.get("TICK", "1.0"))
# MOVE_SCALE は地図アニメーションの再生速度(実時間より少し速く動かすだけ)。
# ※ 偽の時刻加速(以前の TIME_SCALE)は廃止。時刻は実時刻を使う。
MOVE_SCALE = float(os.environ.get("MOVE_SCALE", "8"))
BATCH_POST = os.environ.get("BATCH_POST", "1") != "0"
ADD_REVERSE_EDGES = os.environ.get("ADD_REVERSE_EDGES") == "1"
AVOID_UTURN = os.environ.get("AVOID_UTURN", "1") != "0"
SPAWN_BBOX = os.environ.get("SPAWN_BBOX", "")
SELFTEST = os.environ.get("SELFTEST") == "1"
RANDOM_SEED = os.environ.get("RANDOM_SEED")


def find_graphml() -> str:
    p = os.environ.get("GRAPHML")
    if p:
        if not os.path.exists(p):
            print(f"[feed] GRAPHML not found: {p}", file=sys.stderr)
            sys.exit(1)
        return p
    preferred = "/data/road/road_drive.graphml"
    if os.path.exists(preferred):
        return preferred
    print("[feed] no /data/road/road_drive.graphml. run `make data` or set GRAPHML explicitly.", file=sys.stderr)
    sys.exit(1)


def parse_bbox(value: str):
    if not value:
        return None
    try:
        a, b, c, d = [float(x.strip()) for x in value.split(",")]
    except ValueError:
        print("[feed] SPAWN_BBOX must be 'min_lat,min_lon,max_lat,max_lon'", file=sys.stderr)
        sys.exit(1)
    return min(a, c), min(b, d), max(a, c), max(b, d)


def choose_start_nodes(nodes, adj, bbox):
    active = [n for n in adj if adj[n]]
    if not bbox:
        return active
    min_lat, min_lon, max_lat, max_lon = bbox
    scoped = [
        n for n in active
        if min_lat <= nodes[n][0] <= max_lat and min_lon <= nodes[n][1] <= max_lon
    ]
    if scoped:
        return scoped
    print("[feed] SPAWN_BBOX matched no drivable nodes; falling back to whole graph", file=sys.stderr)
    return active


def haversine(a, b) -> float:
    R = 6371000.0
    (la1, lo1), (la2, lo2) = a, b
    p1, p2 = math.radians(la1), math.radians(la2)
    dp, dl = math.radians(la2 - la1), math.radians(lo2 - lo1)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(x))


def poly_len(poly) -> float:
    return sum(haversine(poly[i], poly[i + 1]) for i in range(len(poly) - 1))


def point_at(poly, d: float):
    if d <= 0:
        return poly[0]
    for i in range(len(poly) - 1):
        seg = haversine(poly[i], poly[i + 1])
        if d <= seg or i == len(poly) - 2:
            f = d / seg if seg > 0 else 0.0
            return (poly[i][0] + (poly[i + 1][0] - poly[i][0]) * f,
                    poly[i][1] + (poly[i + 1][1] - poly[i][1]) * f)
        d -= seg
    return poly[-1]


def real_clock():
    """実時刻の (hour 0-23, dow 1-7)。偽の加速はしない。"""
    dt = datetime.now()
    return dt.hour, dt.isoweekday()


def edge_speed(maxspeed: float, road_type: str, hour: int, link_id: str) -> float:
    # 共有の渋滞モデルを使う(学習データ生成 trainsim.py と同一)。
    return congestion.edge_speed(maxspeed, road_type, hour, congestion.link_factor(link_id))


class Agent:
    def __init__(self, vid, adj, start_nodes):
        self.vid = vid
        self.adj = adj
        self.start_nodes = start_nodes
        self.prev_node = None
        self.node = random.choice(start_nodes)
        self._pick_edge()

    def _pick_edge(self):
        # 直前に走ってきた辺を退避(行き止まりでの U ターン用)
        came_poly = getattr(self, "poly", None)
        came = (getattr(self, "link_id", None), getattr(self, "road_type", None),
                getattr(self, "maxspeed", None))
        edges = self.adj.get(self.node) or []
        # 前進候補(AVOID_UTURN なら来た道=prev_node へ戻る辺は除く)
        forward = edges
        if AVOID_UTURN and self.prev_node is not None and edges:
            forward = [e for e in edges if e[0] != self.prev_node]

        if forward:
            self.target, self.poly, self.link_id, self.road_type, self.maxspeed = random.choice(forward)
        elif self.prev_node is not None and came_poly is not None:
            # 行き止まり(前進できる道が無い)→ 来た道をそのまま U ターンで引き返す
            self.target = self.prev_node
            self.poly = list(reversed(came_poly))
            self.link_id, self.road_type, self.maxspeed = came
        elif edges:
            # 起動直後など prev_node が無い場合は出口から選ぶ
            self.target, self.poly, self.link_id, self.road_type, self.maxspeed = random.choice(edges)
        else:
            # 出口も来た道も無い孤立ノード(ほぼ起こらない)→ 別ノードへ再配置
            self.prev_node = None
            self.node = random.choice(self.start_nodes)
            self._pick_edge()
            return
        self.edge_len = max(1.0, poly_len(self.poly))
        self.d = 0.0

    def step(self, move_dt, hour):
        remaining = max(0.0, move_dt)
        hops = 0
        while remaining > 0:
            sp = edge_speed(self.maxspeed, self.road_type, hour, self.link_id)
            speed_mps = max(0.1, sp * 1000 / 3600)
            edge_remaining = max(0.0, self.edge_len - self.d)
            travel = speed_mps * remaining
            if travel < edge_remaining:
                self.d += travel
                break
            remaining -= edge_remaining / speed_mps
            arrived_from = self.node
            self.node = self.target
            self.prev_node = arrived_from
            self._pick_edge()
            hops += 1
            if hops >= 100:
                # 異常に短いリンクが連続しても1 tickで無限に回らないようにする。
                break
        lat, lon = point_at(self.poly, self.d)
        sp = edge_speed(self.maxspeed, self.road_type, hour, self.link_id)
        return lat, lon, sp, self.link_id, self.road_type, self.maxspeed


def post(event: dict) -> bool:
    data = json.dumps(event).encode()
    req = urllib.request.Request(f"{INGEST_URL}/positions", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 201
    except urllib.error.URLError as exc:
        print(f"[feed] POST failed: {exc}", file=sys.stderr)
        return False


def post_batch(events: list[dict]) -> int:
    if not events:
        return 0
    data = json.dumps(events).encode()
    req = urllib.request.Request(f"{INGEST_URL}/positions/batch", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 201:
                return 0
            body = json.loads(resp.read().decode() or "{}")
            return int(body.get("inserted", len(events)))
    except urllib.error.URLError as exc:
        print(f"[feed] batch POST failed: {exc}", file=sys.stderr)
        return 0


def main() -> None:
    if RANDOM_SEED:
        random.seed(int(RANDOM_SEED))
    path = find_graphml()
    nodes, adj = graphml_road.load_graph(path, add_reverse_edges=ADD_REVERSE_EDGES)
    if not any(adj.values()):
        print(f"[feed] graph has no edges: {path}", file=sys.stderr)
        sys.exit(1)
    bbox = parse_bbox(SPAWN_BBOX)
    start_nodes = choose_start_nodes(nodes, adj, bbox)
    agents = [Agent(f"v{i:03d}", adj, start_nodes) for i in range(AGENTS)]
    edge_count = sum(len(edges) for edges in adj.values())
    print(
        f"[feed] loaded {path}: nodes={len(nodes)} edges={edge_count} "
        f"spawn_nodes={len(start_nodes)} agents={len(agents)} batch={BATCH_POST} "
        f"selftest={SELFTEST}",
        flush=True,
    )

    move_dt = TICK * MOVE_SCALE
    sent = ticks = 0
    while True:
        hour, dow = real_clock()
        events = []
        for a in agents:
            lat, lon, sp, link_id, road_type, maxspeed = a.step(move_dt, hour)
            event = {
                "vehicle_id": a.vid,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "speed_kmh": round(sp, 2),
                "link_id": link_id,
                "road_type": road_type,
                "maxspeed": round(maxspeed, 1),
                "sim_hour": hour,
                "sim_dow": dow,
            }
            if SELFTEST:
                if ticks < 2 and a.vid == "v000":
                    print("  sample:", event)
            elif BATCH_POST:
                events.append(event)
            elif post(event):
                sent += 1
        if not SELFTEST and BATCH_POST:
            sent += post_batch(events)
        ticks += 1
        if SELFTEST and ticks >= 24:
            break
        if not SELFTEST and sent and sent % 1000 < len(agents):
            print(f"[feed] sent ~{sent} (sim_hour={hour})", flush=True)
        time.sleep(TICK)


if __name__ == "__main__":
    main()
