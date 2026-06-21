# 操作用ショートカット
#
# 通常の流れ:
#   make up        # 基盤 + stream-scorer + Grafana を起動 (healthcheck まで待機)
#   make connector # Debezium コネクタを登録 (CDC 開始)
#   make clients   # ペルソナ別クライアントを起動 (データ投入開始)
#   make train     # Spark 前処理 -> Trainer 学習 (モデル生成 / 再学習)
#   open http://localhost:3000   # Grafana ダッシュボード
#   make down      # 後始末
#
# 一括: make all  (up -> connector -> clients -> train まで)

.PHONY: build up start stop ps wait connector clients clients-stop train verify \
        artifacts dashboard all logs down clean

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
	w "Grafana"       "http://localhost:3000/api/health"'
	@echo "==> 準備完了 (Grafana: http://localhost:3000)"

connector:
	@$(MAKE) --no-print-directory wait
	@echo "==> Debezium コネクタを登録..."
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @03_debezium/register-postgres.json \
		http://localhost:8083/connectors | sed 's/,/,\n/g' || true
	@echo "\nコネクタ状態:"
	@curl -sf http://localhost:8083/connectors/oltp-postgres-connector/status || true

# ペルソナ別クライアントを起動(非同期にデータを投入し続ける)
clients-start:
	docker compose --profile clients up -d --build
	@echo "==> クライアント稼働中。停止は: make clients-stop"

clients-stop:
	docker compose --profile clients stop

train:
	docker compose run --rm --build spark
	docker compose run --rm --build trainer

verify:
	@echo "PostgreSQL 行数:"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(*) FROM transactions;"
	@echo "ClickHouse: transactions / predictions / model_runs:"
	@docker compose exec -T clickhouse clickhouse-client -q \
		"SELECT 'transactions' AS t, count() FROM dwh.transactions UNION ALL SELECT 'predictions', count() FROM dwh.predictions UNION ALL SELECT 'model_runs', count() FROM dwh.model_runs"

# parquet とモデルの保存状況(更新時刻・サイズ・学習指標)を要約表示
artifacts:
	@docker run --rm -v mlops_bi_streaming_features:/data -v mlops_bi_streaming_models:/models alpine sh -c '\
		echo "===== parquet (features volume) ====="; \
		if [ -d /data/transactions ]; then \
			echo "  更新時刻 : $$(date -r /data/transactions "+%Y-%m-%d %H:%M:%S")"; \
			echo "  part 数  : $$(ls /data/transactions/*.parquet 2>/dev/null | wc -l) files"; \
			echo "  合計     : $$(du -sh /data/transactions | cut -f1)"; \
		else echo "  (まだありません — make train を実行)"; fi; \
		echo "===== model (models volume) ====="; \
		if [ -f /models/model.joblib ]; then \
			echo "  更新時刻 : $$(date -r /models/model.joblib "+%Y-%m-%d %H:%M:%S")"; \
			echo "  サイズ   : $$(du -h /models/model.joblib | cut -f1)"; \
			echo "  指標     : $$(grep -E "n_train|n_test|roc_auc" /models/metadata.json | tr -d " \"," | tr "\n" " ")"; \
		else echo "  (まだありません — make train を実行)"; fi'

dashboard:
	@echo "Grafana ダッシュボード: http://localhost:3000/d/fraud-overview"

# --- 一括 -------------------------------------------------------------------
all:
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory connector
	@$(MAKE) --no-print-directory clients
	@echo "==> データが溜まるのを待機 (20秒)..."
	@sleep 20
	@$(MAKE) --no-print-directory train
	@echo "==> 完了。Grafana を開く: http://localhost:3000/d/fraud-overview"

logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile clients --profile batch down

clean:
	docker compose --profile clients --profile batch down -v
