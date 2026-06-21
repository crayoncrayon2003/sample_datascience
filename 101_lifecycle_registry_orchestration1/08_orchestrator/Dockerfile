# Dagster オーケストレータ。
# パイプラインの各ステップ(Spark 前処理 / 学習 / 昇格)は既存コンテナとして実装済みなので、
# Dagster 自身は計算を持たず「docker compose run でそれらを起動する指揮者」に徹する。
# そのため docker CLI + compose plugin を同梱し、compose の socket をマウントして使う。
#
# 想定アーキテクチャ: linux/amd64(x86_64)。arm64 ではバイナリ URL を読み替えること。
FROM python:3.12-slim

ARG DOCKER_VERSION=27.3.1
ARG COMPOSE_VERSION=v2.29.7

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" \
        | tar xz --strip-components=1 -C /usr/local/bin docker/docker \
    && install -d /usr/local/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY definitions.py .

EXPOSE 3001

# `dagster dev` は webserver + daemon(スケジュール実行)を一括起動する。
# 本サンプル用途では十分。永続化が必要なら DAGSTER_HOME + dagster.yaml を足す。
# -f は絶対パスにする(compose 側で working_dir を /project にしているため、
#  相対 "definitions.py" だと /project/definitions.py を探して FileNotFound になる)。
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3001", "-f", "/app/definitions.py"]
