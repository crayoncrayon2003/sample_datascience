# Docker 公式ライブラリの Spark イメージ(PySpark 用に python3 同梱タグ)
FROM spark:3.5.8-python3

USER root

# ClickHouse JDBC ドライバ(0.7.x の all = 依存込み。0.6.5 は壊れているので使わない)
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2-all.jar \
    /opt/spark/jars/clickhouse-jdbc.jar
RUN chmod 644 /opt/spark/jars/clickhouse-jdbc.jar

COPY jobs /opt/jobs
ENTRYPOINT []
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/jobs/preprocess.py"]
