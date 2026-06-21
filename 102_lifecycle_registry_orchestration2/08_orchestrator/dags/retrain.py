"""Airflow DAG: 再学習パイプライン(features → challenger_model → champion_model).

features → challenger_model → champion_model の3ステップを Airflow の DAG で組んだもの。
各タスクは `docker compose run --rm --no-deps <service>` で既存コンテナを1回だけ起動するだけ
(Airflow 自身は計算を持たない指揮者)。--no-deps が要点で、スケジュール実行のたびに
稼働中の基盤(clickhouse / kafka / mlflow)を作り直して止めるのを防ぐ。

  - features         : 05_spark(ClickHouse の dwh.transactions → 特徴量 parquet)
  - challenger_model : 07_trainer/train.py(学習 → MLflow に version 登録 → @challenger)
  - champion_model   : 07_trainer/promote.py(champion と ROC-AUC 比較 → 勝てば @champion 昇格)
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# スケジュール・表示はすべて日本時間(JST)で扱う(イメージの ENV でも TZ=Asia/Tokyo を設定)。
JST = pendulum.timezone("Asia/Tokyo")

# compose 一式をマウントした場所(docker-compose.yml の volumes: ./:/project)
PROJECT_DIR = "/project"


def compose_run(service: str) -> str:
    """ホストの docker に対し、既存サービスを1回だけ起動するコマンド文字列。"""
    return f"cd {PROJECT_DIR} && docker compose run --rm --no-deps {service}"


with DAG(
    dag_id="retrain_pipeline",
    description="features → challenger_model → champion_model",
    schedule="*/10 * * * *",          # 10 分ごとに再学習(JST 基準)
    start_date=pendulum.datetime(2024, 1, 1, tz=JST),
    catchup=False,                     # 過去ぶんをまとめ実行しない
    max_active_runs=1,                 # 同時実行を1本に制限
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["mlops", "registry", "lifecycle"],
) as dag:
    features = BashOperator(
        task_id="features",
        bash_command=compose_run("spark"),
    )
    challenger_model = BashOperator(
        task_id="challenger_model",
        bash_command=compose_run("trainer"),
    )
    champion_model = BashOperator(
        task_id="champion_model",
        bash_command=compose_run("promoter"),
    )

    features >> challenger_model >> champion_model
