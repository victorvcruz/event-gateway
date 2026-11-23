# ─────────────────────────────────────────────────────────────────────────────
# Event Gateway — Makefile
#
# Ambiente local via docker-compose em infrastructure/. Endpoints publicados:
#
#   Componente       | URL                                | Notas
#   -----------------|------------------------------------|------------------
#   gateway-server   | http://localhost:8080/events       | POST envelopes
#                    | http://localhost:8080/metrics      | Prometheus (:9090)
#                    | http://localhost:8080/healthz      |
#   gateway-worker   | http://localhost:9091/metrics      | Prometheus
#   gateway-db       | postgres://gateway:secret@         | PostgreSQL 16
#                    |   localhost:5438/gateway_db        |
#   Kafka broker     | localhost:9092                     | KRaft-nativo
#   Kafka UI         | http://localhost:8180              | Provectus
#   Prometheus       | http://localhost:9099              | scrape 15s
#   Grafana          | http://localhost:3000              | anônimo Editor
#                    |                                    | ou admin/admin
#                    | Explore → Loki                     | logs dos containers
#                    | Explore → Prometheus               | métricas
#   Loki             | http://localhost:3100              | armazenamento logs
#   Promtail         | interno                            | colhe /var/lib/docker
#
# Fluxo típico de trabalho:
#   1. make up                         → sobe a stack (Postgres, Kafka, Gateway, Grafana)
#   2. make smoke                      → valida ingest + relay + kafka
#   3. curl http://localhost:8080/...  → exercita endpoints
#   4. abrir http://localhost:3000/    → Grafana com 5 dashboards preloaded
#   5. make down                       → limpa
# ─────────────────────────────────────────────────────────────────────────────

SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

MODULES     := shared gateway order-service payment-service inventory-service fulfillment-service notification-service
COMPOSE_DIR := infrastructure
COMPOSE_PROJECT_NAME ?= infrastructure
COMPOSE     := docker compose --project-name $(COMPOSE_PROJECT_NAME) -f $(COMPOSE_DIR)/docker-compose.yml
GATEWAY_URL ?= http://localhost:8080
WORKER_URL  ?= http://localhost:9091
DB_URL      ?= postgres://gateway:secret@localhost:5438/gateway_db?sslmode=disable

.PHONY: help \
        up up-infra up-gateway down down-clean restart logs ps \
        rebuild-gateway \
        generate verify verify-sdk-deps test tidy lint migrations clean \
        smoke smoke-saga smoke-compensations smoke-out-of-order metrics logs-server logs-worker grafana prometheus db-shell kafka-ui \
        schemas-register schemas-list schemas-compat-test \
        chaos-up chaos chaos-verification chaos-resilience chaos-crash chaos-scenario chaos-down chaos-diagnostics chaos-harness-test verify-chaos \
        experiments-up experiments-down experiments-diagnostics experiments-dry-run experiment experiments experiments-c-extended experiments-analyze experiments-test

# ─────────────────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────────────────

help:
	@printf '%s\n' \
	  'Event Gateway — targets disponíveis:' \
	  '' \
	  ' Stack (docker-compose):' \
	  '   up                    sobe a stack completa (Postgres, Kafka, Gateway, Prometheus, Grafana)' \
	  '   up-infra              sobe só a infra base (Postgres + Kafka + Kafka UI + Schema Registry)' \
	  '   up-gateway            sobe só o Gateway (server + worker) — depende de up-infra' \
	  '   down                  para e remove containers (mantém volumes)' \
	  '   down-clean            para e remove containers E volumes (dados perdidos)' \
	  '   restart               restart de gateway-server + gateway-worker' \
	  '   rebuild-gateway       rebuild + restart do gateway-server e gateway-worker (após code change)' \
	  '   logs                  segue logs de gateway-server e gateway-worker' \
	  '   ps                    docker compose ps' \
	  '' \
	  ' Observabilidade:' \
	  '   grafana               abre Grafana (http://localhost:3000) com 5 dashboards preloaded' \
	  '   prometheus            abre Prometheus (http://localhost:9099)' \
	  '   kafka-ui              abre Kafka UI (http://localhost:8180)' \
	  '   schemas-register      publica os nove contratos no Schema Registry' \
	  '   schemas-list          lista subjects e versões registrados' \
	  '   schemas-compat-test   demonstra BACKWARD, FORWARD, FULL e FULL_TRANSITIVE' \
	  '   metrics               curl :9091/metrics (worker) filtrado por gateway_*' \
	  '   logs-server           logs do gateway-server via Loki (últimos 100)' \
	  '   logs-worker           logs do gateway-worker via Loki (últimos 100)' \
	  '' \
	  ' Banco:' \
	  '   db-shell              psql interativo no gateway_db' \
	  '   migrations            aplica migrations (shared/outbox base + dispatch_outbox + gateway)' \
	  '' \
	  ' Testes e verificação:' \
	  '   smoke                 roda tests/smoke.sh (ingest → outbox → Kafka)' \
	  '   smoke-saga            cria pedido e aguarda a saga completa' \
	  '   smoke-compensations   valida falhas de pagamento/estoque e idempotência' \
	  '   smoke-out-of-order    valida compensações recebidas antes de order.created' \
	  '   chaos                 executa a suíte de caos em stack descartável' \
	  '   chaos-resilience      executa somente os cenários determinísticos/Toxiproxy' \
	  '   chaos-crash           executa crash windows, Kafka rewind e contenção' \
	  '   chaos-scenario        executa SCENARIO=<nome> em stack descartável' \
	  '   chaos-up/down         cria ou remove manualmente a stack de caos' \
	  '   chaos-diagnostics     mostra estado, logs e registros da stack de caos' \
	  '   chaos-harness-test    testa lock, estado e timeouts sem Docker real' \
	  '   experiments-up/down   cria ou remove a stack quantitativa isolada' \
	  '   experiments-dry-run   executa A-D com 1x10 e outages de 3s' \
	  '   experiment            executa EXPERIMENT=A|B|C|D (5x100)' \
	  '   experiments           executa e promove a coleta oficial A-D (5x100)' \
	  '   experiments-c-extended execucao suplementar do Exp. C sem pausar o worker (nao oficial)' \
	  '   experiments-analyze   reanalisa RUN_ID=<id>' \
	  '   test                  go test ./... em cada módulo (via testcontainers-go)' \
	  '   verify                go build ./... em cada módulo com GOWORK=off (build isolado)' \
	  '   verify-sdk-deps       garante que o SDK não importa Kafka/pgx/internal' \
	  '   lint                  golangci-lint run em cada módulo' \
	  '   generate              go generate ./... em cada módulo (regenera mocks)' \
	  '   tidy                  go mod tidy em cada módulo' \
	  '' \
	  ' Higiene:' \
	  '   clean                 remove binários locais em bin/'

# ─────────────────────────────────────────────────────────────────────────────
# Stack — docker-compose
# ─────────────────────────────────────────────────────────────────────────────

up:
	@echo "==> subindo stack completa"
	@$(COMPOSE) up -d

up-infra:
	@echo "==> subindo infra base (Postgres + Kafka + Kafka UI + Schema Registry)"
	@$(COMPOSE) up -d gateway-db kafka kafka-ui schema-registry kafka-init

up-gateway: up-infra
	@echo "==> subindo Gateway (server + worker + migrations)"
	@$(COMPOSE) up -d schema-init gateway-migrations gateway-server gateway-worker

schemas-register:
	@echo "==> registrando contratos JSON Schema"
	@$(COMPOSE) run --rm --build schema-init

schemas-list:
	@echo "==> subjects do Schema Registry"
	@curl -fsS "$${SCHEMA_REGISTRY_URL:-http://localhost:$${SCHEMA_REGISTRY_PORT:-8181}}/subjects" | python3 -m json.tool

schemas-compat-test:
	@SCHEMA_REGISTRY_URL="$${SCHEMA_REGISTRY_URL:-http://localhost:$${SCHEMA_REGISTRY_PORT:-8181}}" ./tests/schema-compat.sh

down:
	@echo "==> derrubando stack (volumes preservados)"
	@$(COMPOSE) down

down-clean:
	@echo "==> derrubando stack E removendo volumes (dados perdidos)"
	@$(COMPOSE) down -v

restart:
	@echo "==> restart de gateway-server + gateway-worker"
	@$(COMPOSE) restart gateway-server gateway-worker

rebuild-gateway:
	@echo "==> rebuild + restart do Gateway (após code change)"
	@$(COMPOSE) up -d --build gateway-server gateway-worker

logs:
	@$(COMPOSE) logs -f gateway-server gateway-worker

ps:
	@$(COMPOSE) ps

# ─────────────────────────────────────────────────────────────────────────────
# Observabilidade
# ─────────────────────────────────────────────────────────────────────────────

grafana:
	@echo "abrindo Grafana em http://localhost:3000 (login: admin/admin ou anônimo)"
	@open http://localhost:3000 2>/dev/null || echo "abra manualmente: http://localhost:3000"

prometheus:
	@echo "abrindo Prometheus em http://localhost:9099"
	@open http://localhost:9099 2>/dev/null || echo "abra manualmente: http://localhost:9099"

kafka-ui:
	@echo "abrindo Kafka UI em http://localhost:8180"
	@open http://localhost:8180 2>/dev/null || echo "abra manualmente: http://localhost:8180"

metrics:
	@echo "==> gateway-worker $(WORKER_URL)/metrics"
	@curl -s $(WORKER_URL)/metrics | grep -E '^gateway_' | sort | head -80

logs-server:
	@echo "==> últimos 100 logs de gateway-server via Loki"
	@curl -s --data-urlencode 'query={compose_project="$(COMPOSE_PROJECT_NAME)",compose_service="gateway-server"}' --data-urlencode 'limit=100' 'http://localhost:3100/loki/api/v1/query_range' \
	  | python3 -c "import sys,json;\
	   d=json.load(sys.stdin);\
	   [print(v[1]) for s in d.get('data',{}).get('result',[]) for v in s['values']]"

logs-worker:
	@echo "==> últimos 100 logs de gateway-worker via Loki"
	@curl -s --data-urlencode 'query={compose_project="$(COMPOSE_PROJECT_NAME)",compose_service="gateway-worker"}' --data-urlencode 'limit=100' 'http://localhost:3100/loki/api/v1/query_range' \
	  | python3 -c "import sys,json;\
	   d=json.load(sys.stdin);\
	   [print(v[1]) for s in d.get('data',{}).get('result',[]) for v in s['values']]"

# ─────────────────────────────────────────────────────────────────────────────
# Banco
# ─────────────────────────────────────────────────────────────────────────────

db-shell:
	@echo "==> psql interativo em gateway-db (\\q para sair)"
	@$(COMPOSE) exec gateway-db psql -U gateway -d gateway_db

# ─────────────────────────────────────────────────────────────────────────────
# Testes
# ─────────────────────────────────────────────────────────────────────────────

smoke:
	@echo "==> smoke: POST /events → outbox → Kafka"
	@bash tests/smoke.sh

smoke-saga:
	@echo "==> smoke saga: order → payment + inventory → fulfillment"
	@bash tests/saga-smoke.sh

smoke-compensations:
	@echo "==> smoke compensations: payment.failed + stock.failed"
	@bash tests/saga-compensations.sh

smoke-out-of-order:
	@echo "==> smoke out-of-order compensation markers"
	@bash tests/out-of-order.sh

chaos-up:
	@bash tests/chaos/control.sh up

chaos:
	@bash tests/chaos/run.sh

chaos-verification:
	@CHAOS_SUITE=verification bash tests/chaos/run.sh

chaos-resilience:
	@CHAOS_SUITE=resilience bash tests/chaos/run.sh

chaos-crash:
	@CHAOS_SUITE=crash bash tests/chaos/run.sh

chaos-scenario:
	@test -n "$(SCENARIO)" || { echo "uso: make chaos-scenario SCENARIO=lost-ack" >&2; exit 2; }
	@SCENARIO="$(SCENARIO)" bash tests/chaos/run.sh

chaos-down:
	@bash tests/chaos/control.sh down

chaos-diagnostics:
	@bash tests/chaos/control.sh diagnostics "$(ORDER_ID)"

chaos-harness-test:
	@bash tests/chaos/harness_test.sh

experiments-up:
	@python3 experiments/runner.py up

experiments-down:
	@python3 experiments/runner.py down

experiments-diagnostics:
	@python3 experiments/runner.py diagnostics "$(RUN_ID)"

experiments-dry-run:
	@python3 experiments/runner.py run --experiment ALL --samples 10 --repetitions 1 --warmups 2 --outages 3

experiment:
	@test "$(EXPERIMENT)" = A -o "$(EXPERIMENT)" = B -o "$(EXPERIMENT)" = C -o "$(EXPERIMENT)" = D || { echo "uso: make experiment EXPERIMENT=A|B|C|D [RESUME=<run_id>]" >&2; exit 2; }
	@python3 experiments/runner.py run --experiment "$(EXPERIMENT)" $(if $(RESUME),--resume "$(RESUME)",)

experiments:
	@python3 experiments/runner.py run --experiment ALL --official $(if $(RESUME),--resume "$(RESUME)",)

# Execução suplementar (não oficial) do Experimento C: o worker reenviador NÃO é
# pausado e a política de tentativas é ampliada (10 tentativas, teto de 30 s),
# para janelas de 60 e 120 s. Usa run_id próprio; não altera a coleta oficial.
experiments-c-extended:
	@python3 experiments/runner.py run --experiment C --retry-policy extended --outages 60,120

experiments-analyze:
	@test -n "$(RUN_ID)" || { echo "uso: make experiments-analyze RUN_ID=<id> [PROMOTE=1]" >&2; exit 2; }
	@python3 experiments/analysis.py "$(RUN_ID)" $(if $(PROMOTE),--promote,)

experiments-test:
	@python3 -m unittest discover -s experiments/tests -v

smoke-sdk:
	@echo "==> smoke SDK gateway/pkg/client contra $(GATEWAY_URL)"
	@GATEWAY_URL=$(GATEWAY_URL) bash tests/smoke-sdk.sh 2>/dev/null || echo "(criar tests/smoke-sdk.sh para automatizar; ver quickstart §8b para exemplo Go standalone)"

# ─────────────────────────────────────────────────────────────────────────────
# Build & test dos módulos Go
# ─────────────────────────────────────────────────────────────────────────────

generate:
	@status=0; for m in $(MODULES); do echo "==> generate $$m"; (cd $$m && go generate ./...) || status=1; done; exit $$status

tidy:
	@status=0; for m in $(MODULES); do echo "==> tidy $$m"; (cd $$m && go mod tidy) || status=1; done; exit $$status

verify:
	@status=0; for m in $(MODULES); do echo "==> build $$m (GOWORK=off)"; (cd $$m && GOWORK=off go build ./...) || status=1; done; exit $$status

verify-chaos:
	@status=0; for m in $(MODULES); do echo "==> build $$m com tag chaos (GOWORK=off)"; (cd $$m && GOWORK=off go build -tags chaos ./...) || status=1; done; exit $$status

verify-sdk-deps:
	@echo "==> verify-sdk-deps: SDK sem dependências proibidas"
	@bad=$$(cd gateway && go list -deps ./pkg/client ./pkg/dispatcher | grep -E '(franz-go|jackc/pgx|event-gateway/gateway/internal/|event-gateway/shared/)' || true); \
	if [ -n "$$bad" ]; then \
		echo "FAIL: gateway/pkg importou dependências proibidas:"; \
		echo "$$bad"; \
		exit 1; \
	fi; \
	echo "OK: gateway/pkg limpo"

test:
	@status=0; for m in $(MODULES); do echo "==> test $$m"; (cd $$m && go test ./... -count=1) || status=1; done; exit $$status

lint:
	@status=0; for m in $(MODULES); do echo "==> lint $$m"; (cd $$m && GOWORK=off golangci-lint run ./...) || status=1; done; exit $$status

migrations:
	@ran=0; \
	for m in $(MODULES); do \
		if [ -f $$m/cmd/migrations/main.go ]; then \
			echo "==> migrations $$m"; \
			(cd $$m && go run ./cmd/migrations); \
			ran=$$((ran+1)); \
		fi; \
	done; \
	if [ $$ran -eq 0 ]; then \
		echo "no cmd/migrations found in any module"; \
		exit 1; \
	fi

clean:
	@rm -rf bin/
