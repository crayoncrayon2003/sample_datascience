# 102_lifecycle_registry_orchestration2 操作用ショートカット
#
# 通常の流れ:
#   make up        # 基盤 + mlflow + airflow + stream-scorer + Grafana を起動
#   make connector # Debezium コネクタを登録 (CDC 開始)
#   make clients   # ペルソナ別クライアントを起動 (データ投入開始)
#   make train     # 前処理 -> 学習(@challenger) -> 昇格(@champion) を一括実行
#   open http://localhost:5000   # MLflow(実験 / Model Registry)
#   open http://localhost:8080   # Airflow(DAG retrain_pipeline / スケジュール)
#   make airflow-pass            # Airflow admin のパスワードを表示
#   open http://localhost:3000   # Grafana ダッシュボード
#   make down      # 後始末
#
# 一括: make all  (up -> connector -> clients -> train まで)
#
# 再学習は Airflow が 10 分ごとに自動実行する(features -> challenger -> champion)。
# 手動で 1 周回したいときは make train。

.PHONY: build up start stop ps wait connector clients clients-start clients-stop \
        train preprocess promote registry verify artifacts dashboard mlflow airflow airflow-pass \
        all logs down clean

build:
	docker compose build

up: build
	docker compose up -d
	@$(MAKE) --no-print-directory wait

start:
	docker compose start
	@$(MAKE) --no-print-directory wait

stop:
	docker compose stop

ps:
	docker compose ps

# 各サービスの healthcheck エンドポイントが応答するまでブロックする
wait:
	@echo "==> サービスの起動を待機中 (最大180秒)..."
	@bash -c 'w() { \
	  printf "  %-16s" "$$1"; \
	  for i in $$(seq 1 60); do \
	    if curl -sf -o /dev/null "$$2"; then echo "OK"; return 0; fi; \
	    sleep 3; \
	  done; \
	  echo "TIMEOUT ($$2)"; return 1; \
	}; \
	w "ClickHouse"    "http://localhost:8123/ping"     && \
	w "Kafka Connect" "http://localhost:8083/"         && \
	w "Ingest API"    "http://localhost:8000/health"   && \
	w "MLflow"        "http://localhost:5000/health"   && \
	w "Airflow"       "http://localhost:8080/health"   && \
	w "Grafana"       "http://localhost:3000/api/health"'
	@echo "==> 準備完了 (MLflow :5000 / Airflow :8080 / Grafana :3000)"

connector:
	@$(MAKE) --no-print-directory wait
	@echo "==> Debezium コネクタを登録..."
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @03_debezium/register-postgres.json \
		http://localhost:8083/connectors | sed 's/,/,\n/g' || true
	@echo "\nコネクタ状態:"
	@curl -sf http://localhost:8083/connectors/oltp-postgres-connector/status || true

# ペルソナ別クライアントを起動(非同期にデータを投入し続ける)
clients: clients-start
clients-start:
	docker compose --profile clients up -d --build
	@echo "==> クライアント稼働中。停止は: make clients-stop"

clients-stop:
	docker compose --profile clients stop

# --- 学習ライフサイクル -----------------------------------------------------
preprocess:
	docker compose run --rm --build spark

# 前処理 -> 学習(@challenger 登録)-> 昇格ゲート(@champion 判定)を 1 周
train:
	docker compose run --rm --build spark
	docker compose run --rm --build trainer
	docker compose run --rm --build promoter

# 昇格ゲートだけを再評価(challenger vs champion)
promote:
	docker compose run --rm --build promoter

# 現在の Registry alias(champion / challenger)を表示
registry:
	@echo "===== MLflow Model Registry: fraud ====="
	@for a in champion challenger; do \
	  v=$$(curl -sf "http://localhost:5000/api/2.0/mlflow/registered-models/alias?name=fraud&alias=$$a" \
	      | grep -o '"version": *"[0-9]*"' | head -1 | grep -o '[0-9]*'); \
	  printf "  @%-11s -> %s\n" "$$a" "$${v:-(未設定)}"; \
	done

verify:
	@echo "PostgreSQL 行数:"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(*) FROM transactions;"
	@echo "ClickHouse: transactions / predictions / model_runs:"
	@docker compose exec -T clickhouse clickhouse-client -q \
		"SELECT 'transactions' AS t, count() FROM dwh.transactions UNION ALL SELECT 'predictions', count() FROM dwh.predictions UNION ALL SELECT 'model_runs', count() FROM dwh.model_runs"

# parquet の保存状況 + Registry の alias を要約表示
artifacts:
	@docker run --rm -v lifecycle_registry_orchestration2_features:/data alpine sh -c '\
		echo "===== parquet (features volume) ====="; \
		if [ -d /data/transactions ]; then \
			echo "  更新時刻 : $$(date -r /data/transactions "+%Y-%m-%d %H:%M:%S")"; \
			echo "  part 数  : $$(ls /data/transactions/*.parquet 2>/dev/null | wc -l) files"; \
			echo "  合計     : $$(du -sh /data/transactions | cut -f1)"; \
		else echo "  (まだありません — make train を実行)"; fi'
	@$(MAKE) --no-print-directory registry

dashboard:
	@echo "Grafana ダッシュボード: http://localhost:3000/d/fraud-overview"

mlflow:
	@echo "MLflow: http://localhost:5000"

airflow:
	@echo "Airflow: http://localhost:8080 (ユーザ admin / パスワードは make airflow-pass)"

# Airflow standalone が生成した admin パスワードを表示
airflow-pass:
	@docker compose exec -T airflow cat /opt/airflow/standalone_admin_password.txt 2>/dev/null \
	  || docker compose logs airflow 2>/dev/null | grep -i "Password for user 'admin'" | tail -1

# --- 一括 -------------------------------------------------------------------
all:
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory connector
	@$(MAKE) --no-print-directory clients
	@echo "==> データが溜まるのを待機 (20秒)..."
	@sleep 20
	@$(MAKE) --no-print-directory train
	@echo "==> 完了。MLflow :5000 / Airflow :8080 / Grafana :3000/d/fraud-overview"

logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile clients --profile batch down

clean:
	docker compose --profile clients --profile batch down -v
