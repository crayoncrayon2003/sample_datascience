# Spark Structured Streaming で Kafka を直接購読しリアルタイムにスコアリングするジョブ。
FROM spark:3.5.8-python3

USER root
# 推論に必要な Python ライブラリ(モデルの unpickle + ClickHouse 書き込み)
RUN python3 -m pip install --no-cache-dir \
    scikit-learn==1.5.2 pandas==2.2.2 joblib==1.4.2 clickhouse-connect==0.7.19

COPY stream_score.py /opt/jobs/stream_score.py
ENTRYPOINT []

# spark-sql-kafka コネクタは --packages で取得(初回起動時に解決)。
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8", \
     "--master", "local[*]", \
     "/opt/jobs/stream_score.py"]
