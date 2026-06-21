"""osmnx が書き出す GraphML 道路グラフを stdlib だけで読む.

networkx/osmnx 不要(feed コンテナを軽量に保つ)。返すのは:
  nodes: {node_id: (lat, lon)}
  adj  : {node_id: [(target_id, polyline[(lat,lon)...], link_id, road_type, maxspeed), ...]}
osmnx のエッジ属性 geometry(実道路形状)/ highway(道路種別)/ maxspeed(制限速度)を読む。
link_id は source/target/edge key から作る方向付きリンクID。OSMnx の drive graph は
oneway を反映した有向グラフなので、勝手に逆向きリンクは追加しない。
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

NS = "{http://graphml.graphdrawing.org/xmlns}"
DEFAULT_MAXSPEED = 40.0
DEFAULT_MAXSPEED_BY_TYPE = {
    "motorway": 100.0, "motorway_link": 60.0,
    "trunk": 60.0, "trunk_link": 50.0,
    "primary": 60.0, "primary_link": 50.0,
    "secondary": 50.0, "secondary_link": 40.0,
    "tertiary": 40.0, "tertiary_link": 40.0,
    "residential": 40.0, "unclassified": 40.0, "service": 30.0,
}


def _parse_linestring(s: str) -> list[tuple[float, float]]:
    inside = s[s.index("(") + 1: s.rindex(")")]
    pts = []
    for pair in inside.split(","):
        lon, lat = pair.strip().split()[:2]
        pts.append((float(lat), float(lon)))
    return pts


def _default_maxspeed(road_type: str) -> float:
    return DEFAULT_MAXSPEED_BY_TYPE.get(road_type, DEFAULT_MAXSPEED)


def _parse_maxspeed(v, road_type: str) -> float:
    """'40' / '40 mph' / "['40','60']" などから数値を取り出す(km/h)。"""
    if not v:
        return _default_maxspeed(road_type)
    nums = re.findall(r"\d+", str(v))
    if not nums:
        return _default_maxspeed(road_type)
    val = float(nums[0])
    if "mph" in str(v).lower():
        val *= 1.60934
    return val


def _parse_highway(v) -> str:
    """highway 値(リスト表記もあり)から代表的な道路種別を1つ返す。"""
    if not v:
        return "unknown"
    s = str(v)
    m = re.findall(r"[a-zA-Z_]+", s)
    return m[0] if m else "unknown"


def _edge_link_id(source: str, target: str, edge_id: str | None) -> str:
    return f"{source}_{target}_{edge_id}" if edge_id is not None else f"{source}_{target}"


def load_graph(path: str, add_reverse_edges: bool | None = None):
    root = ET.parse(path).getroot()
    keymap = {k.get("id"): k.get("attr.name") for k in root.findall(f"{NS}key")}
    graph = root.find(f"{NS}graph")
    if add_reverse_edges is None:
        add_reverse_edges = graph.get("edgedefault") == "undirected"

    nodes: dict[str, tuple[float, float]] = {}
    for n in graph.findall(f"{NS}node"):
        x = y = None
        for d in n.findall(f"{NS}data"):
            name = keymap.get(d.get("key"))
            if name == "x":
                x = float(d.text)
            elif name == "y":
                y = float(d.text)
        if x is not None and y is not None:
            nodes[n.get("id")] = (y, x)

    adj: dict[str, list] = {nid: [] for nid in nodes}
    for e in graph.findall(f"{NS}edge"):
        s, t = e.get("source"), e.get("target")
        if s not in nodes or t not in nodes:
            continue
        geom = None
        highway = maxspeed = None
        for d in e.findall(f"{NS}data"):
            name = keymap.get(d.get("key"))
            if name == "geometry" and d.text:
                try:
                    geom = _parse_linestring(d.text)
                except Exception:  # noqa: BLE001
                    geom = None
            elif name == "highway":
                highway = d.text
            elif name == "maxspeed":
                maxspeed = d.text
        poly = geom if geom and len(geom) >= 2 else [nodes[s], nodes[t]]
        link_id = _edge_link_id(s, t, e.get("id"))
        road_type = _parse_highway(highway)
        spd = _parse_maxspeed(maxspeed, road_type)
        adj[s].append((t, poly, link_id, road_type, spd))
        if add_reverse_edges:
            adj[t].append((s, list(reversed(poly)), link_id, road_type, spd))

    return nodes, adj
