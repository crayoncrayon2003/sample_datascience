# 103_lifecycle_monitoring_drift 操作用ショートカット
#
# 通常の流れ:
#   make up        # 基盤 + stream-scorer + monitor + Grafana を起動 (healthcheck まで待機)
#   make connector # Debezium コネクタを登録 (CDC 開始)
#   make clients   # ペルソナ別クライアントを起動 (正常分布のデータ投入)
#   make train     # Spark 前処理 -> Trainer 学習。この時点の分布が「ドリフトの基準」になる
#   open http://localhost:3000/d/drift-overview   # ドリフト監視ダッシュボード
#   make drift     # ★ドリフト注入クライアントを起動 → ダッシュボードでドリフトが上がるのを見る
#   make down      # 後始末
#
# 一括: make all  (up -> connector -> clients -> train まで)

.PHONY: build up start stop ps wait connector clients clients-start clients-stop \
        drift drift-stop train verify artifacts dashboard all logs down clean

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
	@echo "==> 準備完了 (Grafana: http://localhost:3000/d/drift-overview)"

connector:
	@$(MAKE) --no-print-directory wait
	@echo "==> Debezium コネクタを登録..."
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @03_debezium/register-postgres.json \
		http://localhost:8083/connectors | sed 's/,/,\n/g' || true
	@echo "\nコネクタ状態:"
	@curl -sf http://localhost:8083/connectors/oltp-postgres-connector/status || true

# ペルソナ別クライアントを起動(正常分布。非同期にデータを投入し続ける)
clients: clients-start
clients-start:
	docker compose --profile clients up -d --build
	@echo "==> クライアント稼働中。停止は: make clients-stop"

clients-stop:
	docker compose --profile clients stop

# ★ドリフト注入: 起動直後から分布をズラすクライアント(高額化・高リスク国/カテゴリ・不正増)
#   先に make train で基準を作っておくと、監視ダッシュボードでドリフトが上がるのが見える。
drift:
	docker compose --profile drift up -d --build client-drift
	@echo "==> ドリフト注入中。停止は: make drift-stop / 監視: http://localhost:3000/d/drift-overview"

drift-stop:
	docker compose --profile drift stop

train:
	docker compose run --rm --build spark
	docker compose run --rm --build trainer

verify:
	@echo "PostgreSQL 行数:"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(*) FROM transactions;"
	@echo "ClickHouse: transactions / predictions / drift_metrics:"
	@docker compose exec -T clickhouse clickhouse-client -q \
		"SELECT 'transactions' AS t, count() FROM dwh.transactions UNION ALL SELECT 'predictions', count() FROM dwh.predictions UNION ALL SELECT 'drift_metrics', count() FROM dwh.drift_metrics"
	@echo "最新のドリフト指標:"
	@docker compose exec -T clickhouse clickhouse-client -q \
		"SELECT computed_at, drift_share, dataset_drift, n_drifted, pred_fraud_rate FROM dwh.drift_metrics ORDER BY computed_at DESC LIMIT 1 FORMAT Vertical"

# parquet とモデルの保存状況(更新時刻・サイズ・学習指標)を要約表示
artifacts:
	@docker run --rm -v lifecycle_monitoring_drift_features:/data -v lifecycle_monitoring_drift_models:/models alpine sh -c '\
		echo "===== parquet (features volume / ドリフトの基準) ====="; \
		if [ -d /data/transactions ]; then \
			echo "  更新時刻 : $$(date -r /data/transactions "+%Y-%m-%d %H:%M:%S")"; \
			echo "  part 数  : $$(ls /data/transactions/*.parquet 2>/dev/null | wc -l) files"; \
		else echo "  (まだありません — make train を実行)"; fi; \
		echo "===== model (models volume) ====="; \
		if [ -f /models/model.joblib ]; then \
			echo "  更新時刻 : $$(date -r /models/model.joblib "+%Y-%m-%d %H:%M:%S")"; \
		else echo "  (まだありません — make train を実行)"; fi'

dashboard:
	@echo "ドリフト監視ダッシュボード: http://localhost:3000/d/drift-overview"

# --- 一括 -------------------------------------------------------------------
all:
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory connector
	@$(MAKE) --no-print-directory clients
	@echo "==> データが溜まるのを待機 (20秒)..."
	@sleep 20
	@$(MAKE) --no-print-directory train
	@echo "==> 完了。監視: http://localhost:3000/d/drift-overview  ドリフトを見るなら make drift"

logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile clients --profile drift --profile batch down

clean:
	docker compose --profile clients --profile drift --profile batch down -v
