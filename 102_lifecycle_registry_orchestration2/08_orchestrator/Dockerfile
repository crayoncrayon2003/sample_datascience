# Airflow オーケストレータ。
# パイプラインの各ステップ(Spark 前処理 / 学習 / 昇格)は既存コンテナとして実装済みなので、
# Airflow 自身は計算を持たず「docker compose run でそれらを起動する指揮者」に徹する。
# そのため docker CLI + compose plugin を同梱し、compose の socket をマウントして使う。
#
# 想定アーキテクチャ: linux/amd64(x86_64)。arm64 ではバイナリ URL を読み替えること。
FROM python:3.12-slim

ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.29.7
ARG AIRFLOW_VERSION=2.10.3

# docker CLI + compose plugin(ホストの docker を叩いて既存サービスを起動するため)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates tzdata \
    && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
        | tar xz --strip-components=1 -C /usr/local/bin docker/docker \
    && install -d /usr/local/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_HOME=/opt/airflow
# Airflow は公式 constraints 付きで入れる(依存衝突を避ける)。
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

# タイムゾーンを日本(JST)に固定する。
#   - TZ                          : OS / standalone のログ行のタイムスタンプ
#   - AIRFLOW__CORE__DEFAULT_TIMEZONE       : DAG のスケジュール解釈・naive datetime
#   - AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: Web UI の時刻表示
ENV TZ=Asia/Tokyo \
    AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Tokyo \
    AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Tokyo

# サンプル例の DAG を読み込ませない / DAG は最初から有効化しておく。
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

COPY dags/ ${AIRFLOW_HOME}/dags/

EXPOSE 8080

# `airflow standalone` は DB 初期化 + admin ユーザ作成 + webserver(:8080)+ scheduler を
# 一括起動する(SQLite + SequentialExecutor)。
# admin のパスワードは起動ログ / $AIRFLOW_HOME/standalone_admin_password.txt に出る。
CMD ["airflow", "standalone"]
