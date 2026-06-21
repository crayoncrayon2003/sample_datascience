"""Dagster による再学習パイプラインの定義(software-defined assets)。

004 までの再学習は `make train` の手動実行だった。本サンプルでは Dagster が
DAG 化し、スケジュール(既定 10 分ごと)で回す。資産(asset)の依存関係が

    features ─▶ challenger_model ─▶ champion_model

として lineage グラフに可視化される(これが Dagster を選んだ狙い)。

各 asset の実体は「既存コンテナを docker compose run で起動する」だけ。Dagster 自身は
計算を持たない指揮者。compose の socket をマウントしているのでホストの docker を叩く。

  - features         : 05_spark(ClickHouse 読込 → 特徴量 parquet を features ボリュームへ)
  - challenger_model : 07_trainer/train.py(学習 → MLflow 登録 → @challenger 付与)
  - champion_model   : 07_trainer/promote.py(champion と比較し勝てば @champion 昇格)
"""
import subprocess

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

# compose プロジェクト(docker-compose.yml の name:)。ホストの既存スタックに合流するため、
# /project に compose 一式をマウントしてそこから run する。
PROJECT_DIR = "/project"
SCHEDULE_CRON = "*/10 * * * *"  # 10 分ごとに再学習(必要に応じて変更)


def compose_run(context: AssetExecutionContext, service: str) -> None:
    """`docker compose run --rm --no-deps <service>` をホスト docker に対して実行する。

    --no-deps が要点: スケジュール実行のたびに稼働中の依存(clickhouse / kafka / mlflow)を
    作り直して止めてしまうのを防ぐ。基盤は make up 済みの前提で、そこへ接続するだけにする。
    (--build も付けない: 毎回 JDBC jar を取り直して遅く・不安定。イメージは make up / 初回 run で用意)
    """
    cmd = ["docker", "compose", "run", "--rm", "--no-deps", service]
    context.log.info(f"$ {' '.join(cmd)} (cwd={PROJECT_DIR})")
    proc = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
    if proc.stdout:
        context.log.info(proc.stdout.strip())
    if proc.returncode != 0:
        context.log.error(proc.stderr.strip())
        raise RuntimeError(f"service '{service}' failed (exit {proc.returncode})")
    context.log.info(f"service '{service}' done")


@asset(description="ClickHouse の dwh.transactions から特徴量 parquet を生成(Spark)")
def features(context: AssetExecutionContext) -> None:
    compose_run(context, "spark")


@asset(deps=[features], description="学習し MLflow に version 登録、@challenger を付与")
def challenger_model(context: AssetExecutionContext) -> None:
    compose_run(context, "trainer")


@asset(deps=[challenger_model], description="champion と比較し、勝てば @champion へ昇格")
def champion_model(context: AssetExecutionContext) -> None:
    compose_run(context, "promoter")


retrain_job = define_asset_job("retrain_pipeline", selection="*")

retrain_schedule = ScheduleDefinition(
    job=retrain_job,
    cron_schedule=SCHEDULE_CRON,
    name="retrain_every_10min",
    # 既定 RUNNING にして、`dagster dev` の daemon 起動と同時に有効化する
    # (UI でトグルしなくても 10 分ごとに回り始める)。
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[features, challenger_model, champion_model],
    jobs=[retrain_job],
    schedules=[retrain_schedule],
)
