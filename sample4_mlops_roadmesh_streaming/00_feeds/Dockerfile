FROM python:3.12-slim
WORKDIR /app
# trainsim.py が ClickHouse へ直接書き込むため clickhouse-connect を入れる
# (feed.py 自体は stdlib のみで動く)
RUN pip install --no-cache-dir clickhouse-connect==0.7.19
COPY congestion.py graphml_road.py feed.py trainsim.py ./
# 既定はライブフィード。学習データ生成は compose で command: python trainsim.py
CMD ["python", "feed.py"]
