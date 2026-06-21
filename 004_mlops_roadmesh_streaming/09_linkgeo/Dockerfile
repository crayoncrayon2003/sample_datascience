# 道路リンクの中点・進行方位を ClickHouse(dwh.link_geo)へ投入する一発ジョブ。
# 矢印1本/リンク表示の固定座標を作る。graphml は xml.etree で解析(networkx 不要)。
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir clickhouse-connect==0.7.19
COPY graphml_road.py load_link_geo.py ./
CMD ["python", "load_link_geo.py"]
