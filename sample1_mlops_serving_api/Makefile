# sample1_mlops_serving_api 操作用ショートカット
#
# 通常の流れ (すべて make で完結):
#   make up        # ビルド -> 起動 -> healthcheck 完了まで待機
#   make connector # Debezium コネクタを登録 (CDC 開始)
#   make seed      # Client(入口) がデータを流し込む
#   make verify    # ClickHouse に届いたか確認
#   make train     # Spark 前処理 -> Trainer 学習
#   make predict   # Client(出口) が Serving API に予測を要求
#   make down      # 後始末
#
# 一括実行:
#   make all       # up から predict まで一気通貫(CDC 伝播待ちも込み)
#
# 基盤の細かい操作:
#   make build / up / start / stop / ps / wait / logs / down / clean

.PHONY: build up start stop ps wait connector seed verify train predict \
        artifacts all logs down clean

# --- 基盤のライフサイクル ---------------------------------------------------
build:
	docker compose build

up: build
	docker compose up -d
	@$(MAKE) --no-print-directory wait

start:                 ## 既存コンテナを起動 (再ビルドしない)
	docker compose start
	@$(MAKE) --no-print-directory wait

stop:                  ## コンテナを停止 (削除はしない)
	docker compose stop

ps:                    ## サービスの状態一覧
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
	w "ClickHouse"    "http://localhost:8123/ping"   && \
	w "Kafka Connect" "http://localhost:8083/"       && \
	w "Ingest API"    "http://localhost:8000/health"'
	@echo "==> 準備完了"

# --- パイプライン実行 -------------------------------------------------------
connector:
	@$(MAKE) --no-print-directory wait
	@echo "==> Debezium コネクタを登録..."
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @03_debezium/register-postgres.json \
		http://localhost:8083/connectors | sed 's/,/,\n/g' || true
	@echo "\nコネクタ状態:"
	@curl -sf http://localhost:8083/connectors/oltp-postgres-connector/status || true

seed:
	python3 00_client/client_seed.py --n $(or $(N),2000)

verify:
	@echo "PostgreSQL 行数:"
	@docker compose exec -T postgres psql -U app -d oltp -c "SELECT count(*) FROM transactions;"
	@echo "ClickHouse DWH 行数:"
	@docker compose exec -T clickhouse clickhouse-client -q "SELECT count() FROM dwh.transactions"

train:
	docker compose run --rm --build spark
	docker compose run --rm --build trainer

predict:
	python3 08_client/client_predict.py --n $(or $(N),5)

# parquet とモデルの保存状況(更新時刻・サイズ・件数・学習指標)を要約表示。
# part ファイルを全列挙すると見にくいので、件数と合計サイズに丸めている。
#   watch -n 15 make artifacts   で更新される様子をリアルタイムに観察できる
artifacts:
	@docker run --rm -v sample1_mlops_serving_api_features:/data -v sample1_mlops_serving_api_models:/models alpine sh -c '\
		echo "===== parquet (features volume: /data/features) ====="; \
		if [ -d /data/transactions ]; then \
			echo "  更新時刻 : $$(date -r /data/transactions "+%Y-%m-%d %H:%M:%S")"; \
			echo "  part 数  : $$(ls /data/transactions/*.parquet 2>/dev/null | wc -l) files"; \
			echo "  合計     : $$(du -sh /data/transactions | cut -f1)"; \
		else echo "  (まだありません — make train を実行)"; fi; \
		echo "===== model (models volume: /models) ====="; \
		if [ -f /models/model.joblib ]; then \
			echo "  更新時刻 : $$(date -r /models/model.joblib "+%Y-%m-%d %H:%M:%S")"; \
			echo "  サイズ   : $$(du -h /models/model.joblib | cut -f1)"; \
			echo "  指標     : $$(grep -E "n_train|n_test|roc_auc" /models/metadata.json | tr -d " \"," | tr "\n" " ")"; \
		else echo "  (まだありません — make train を実行)"; fi'

# --- 一括実行 ---------------------------------------------------------------
all:
	@$(MAKE) --no-print-directory up
	@$(MAKE) --no-print-directory connector
	@$(MAKE) --no-print-directory seed
	@echo "==> CDC が DWH へ伝播するのを待機 (15秒)..."
	@sleep 15
	@$(MAKE) --no-print-directory verify
	@$(MAKE) --no-print-directory train
	@$(MAKE) --no-print-directory predict

# --- 補助 -------------------------------------------------------------------
logs:
	docker compose logs -f --tail=100

down:
	docker compose --profile batch down

clean:
	docker compose --profile batch down -v
