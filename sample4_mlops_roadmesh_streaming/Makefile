# sample4_mlops_roadmesh_streaming 操作用ショートカット
#
# 通常の流れ:
#   make data      # (1回) osmnx で大津・草津の道路 graphml を取得
#   make up        # 基盤 + stream-scorer + Grafana を起動 (healthcheck まで待機)
#   make connector # Debezium コネクタを登録 (CDC 開始)
#   make linkgeo   # リンクの中点・進行方位を dwh.link_geo へ投入 (地図の矢印用・1回)
#   make feeds     # 道路網エージェントを起動 (車両位置の投入開始)
#   make traindata # 学習データ生成(全時間帯×道路種別 → dwh.vehicle_history)
#   make train     # 学習データ生成 -> Spark集計 -> Trainer 学習
#   open http://localhost:3000/d/roadmesh-overview   # 地図ダッシュボード
#   make down      # 後始末

.PHONY: data build up start stop ps wait connector linkgeo feeds feeds-stop \
        traindata train verify artifacts dashboard logs down clean

# 実データ:osmnx で大津市・草津市の道路網を取得
data:
	docker compose --profile prep run --rm --build prep

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
	@echo "==> 準備完了 (Grafana: http://localhost:3000/d/roadmesh-overview)"

connector:
	@$(MAKE) --no-print-directory wait
	@echo "==> Debezium コネクタを登録..."
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @03_debezium/register-postgres.json \
		http://localhost:8083/connectors | sed 's/,/,\n/g' || true
	@echo "\nコネクタ状態:"
	@curl -sf http://localhost:8083/connectors/oltp-postgres-connector/status || true

# 地図の矢印用: リンクの中点・進行方位を dwh.link_geo へ投入(graphml が必要: 先に make data)
linkgeo:
	docker compose run --rm --build linkgeo

# 道路網エージェントを起動(graphml が必要: 先に make data)
# feeds サービスだけを対象にする(基盤コンテナを再作成して CDC コネクタを飛ばさないため)
feeds:
	docker compose --profile feeds up -d --build feeds
	@echo "==> エージェント稼働中。停止は: make feeds-stop"

feeds-stop:
	docker compose --profile feeds stop

# 学習データ生成(全時間帯×道路種別を合成し dwh.vehicle_history へ。ライブとは別)
traindata:
	docker compose --profile traindata run --rm --build trainsim

# 学習データ生成 → Spark集計 → Trainer 学習
train:
	docker compose --profile traindata run --rm --build trainsim
	docker compose run --rm --build spark
	docker compose run --rm --build trainer

verify:
	@echo "PostgreSQL 行数:"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(*) FROM vehicle_positions;"
	@echo "PostgreSQL 稼働車両数(直近30秒):"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(DISTINCT vehicle_id) FROM vehicle_positions WHERE created_at > now() - interval '30 seconds';"
	@echo "ClickHouse: vehicle_positions / congestion_pred / model_runs:"
	@docker compose exec -T clickhouse clickhouse-client -q \
		"SELECT 'vehicle_positions' AS t, count() FROM dwh.vehicle_positions UNION ALL SELECT 'congestion_pred', count() FROM dwh.congestion_pred UNION ALL SELECT 'model_runs', count() FROM dwh.model_runs"

# parquet とモデルの保存状況(更新時刻・サイズ・学習指標)を要約表示
artifacts:
	@docker run --rm -v sample4_mlops_roadmesh_streaming_features:/data -v sample4_mlops_roadmesh_streaming_models:/models alpine sh -c '\
		echo "===== parquet (features volume) ====="; \
		if [ -d /data/links ]; then \
			echo "  更新時刻 : $$(date -r /data/links "+%Y-%m-%d %H:%M:%S")"; \
			echo "  part 数  : $$(ls /data/links/*.parquet 2>/dev/null | wc -l) files"; \
			echo "  合計     : $$(du -sh /data/links | cut -f1)"; \
		else echo "  (まだありません — make train を実行)"; fi; \
		echo "===== model (models volume) ====="; \
		if [ -f /models/model.joblib ]; then \
			echo "  更新時刻 : $$(date -r /models/model.joblib "+%Y-%m-%d %H:%M:%S")"; \
			echo "  サイズ   : $$(du -h /models/model.joblib | cut -f1)"; \
			echo "  指標     : $$(grep -E "n_train|n_test|accuracy" /models/metadata.json | tr -d " \"," | tr "\n" " ")"; \
		else echo "  (まだありません — make train を実行)"; fi'

dashboard:
	@echo "Grafana ダッシュボード: http://localhost:3000/d/roadmesh-overview"

logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile feeds --profile batch --profile linkgeo --profile prep --profile traindata down

clean:
	docker compose --profile feeds --profile batch --profile linkgeo --profile prep --profile traindata down -v
