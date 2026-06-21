# bitnami/spark は 2025/8 に Bitnami の無料 Docker Hub 配布方針が変わり pull 不可になったため、
# Docker 公式ライブラリの Spark イメージを使う。
# 素の spark:3.5.8 は scala-only で Python が無いので、PySpark 用に python3 同梱タグを使う
#   (Scala ジョブにするなら spark:3.5.8 でよい)
FROM spark:3.5.8-python3

USER root

# ClickHouse JDBC ドライバ(all = 依存込み)を jars に配置。
# 注意: 0.6.5 の -all は壊れており com.clickhouse.client.ClickHouseClient を同梱しない。
#       0.7.x で修正済みなので 0.7.2 を使う(slf4j は Spark 同梱のものを利用)。
# curl が無い場合に備えて ADD のリモート取得を使う
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2-all.jar \
    /opt/spark/jars/clickhouse-jdbc.jar
RUN chmod 644 /opt/spark/jars/clickhouse-jdbc.jar

COPY jobs /opt/jobs

# 公式イメージの entrypoint は driver/executor 用なので無効化し、直接 spark-submit する
ENTRYPOINT []

# DWH から読み出して前処理 parquet を書き出す
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/jobs/preprocess.py"]
