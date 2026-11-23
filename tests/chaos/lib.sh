#!/usr/bin/env bash
set -euo pipefail

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$CHAOS_DIR/../.." && pwd)"
CHAOS_PROJECT="${CHAOS_PROJECT:-event-gateway-chaos}"
if [[ ! "$CHAOS_PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  printf '[chaos] invalid CHAOS_PROJECT: %s\n' "$CHAOS_PROJECT" >&2
  exit 2
fi
CHAOS_LOCK="${TMPDIR:-/tmp}/${CHAOS_PROJECT}.lock"
CHAOS_STATE_FILE="${TMPDIR:-/tmp}/${CHAOS_PROJECT}.state"
CHAOS_SQL_TIMEOUT_SECONDS="${CHAOS_SQL_TIMEOUT_SECONDS:-8}"

export KAFKA_PORT="${KAFKA_PORT:-19092}"
export SCHEMA_REGISTRY_PORT="${SCHEMA_REGISTRY_PORT:-18181}"
export GATEWAY_DB_PORT="${GATEWAY_DB_PORT:-15438}"
export ORDERS_DB_PORT="${ORDERS_DB_PORT:-15433}"
export PAYMENTS_DB_PORT="${PAYMENTS_DB_PORT:-15434}"
export INVENTORY_DB_PORT="${INVENTORY_DB_PORT:-15435}"
export FULFILLMENT_DB_PORT="${FULFILLMENT_DB_PORT:-15436}"
export NOTIFICATIONS_DB_PORT="${NOTIFICATIONS_DB_PORT:-15437}"
export GATEWAY_PORT="${GATEWAY_PORT:-18080}"
export GATEWAY_METRICS_PORT="${GATEWAY_METRICS_PORT:-19090}"
export GATEWAY_WORKER_METRICS_PORT="${GATEWAY_WORKER_METRICS_PORT:-19091}"
export ORDER_PORT="${ORDER_PORT:-18081}"
export PAYMENT_PORT="${PAYMENT_PORT:-18082}"
export INVENTORY_PORT="${INVENTORY_PORT:-18083}"
export FULFILLMENT_PORT="${FULFILLMENT_PORT:-18084}"
export NOTIFICATION_PORT="${NOTIFICATION_PORT:-18085}"
export TOXIPROXY_PORT="${TOXIPROXY_PORT:-18474}"
export CHAOS_CONTROLLER_PORT="${CHAOS_CONTROLLER_PORT:-18475}"
export COMPOSE_PARALLEL_LIMIT="${COMPOSE_PARALLEL_LIMIT:-4}"

ORDER_URL="${ORDER_URL:-http://127.0.0.1:$ORDER_PORT}"
GATEWAY_URL="${GATEWAY_URL:-http://127.0.0.1:$GATEWAY_PORT}"
TOXIPROXY_URL="${TOXIPROXY_URL:-http://127.0.0.1:$TOXIPROXY_PORT}"
CHAOS_CONTROLLER_URL="${CHAOS_CONTROLLER_URL:-http://127.0.0.1:$CHAOS_CONTROLLER_PORT}"
CHAOS_LAST_ORDER=""

COMPOSE=(docker compose --project-name "$CHAOS_PROJECT" \
  -f "$ROOT_DIR/infrastructure/docker-compose.yml" \
  -f "$ROOT_DIR/infrastructure/docker-compose.chaos.yml")

CHAOS_SERVICES=(
  gateway-db orders-db payments-db inventory-db fulfillment-db notifications-db
  kafka kafka-init schema-registry schema-init toxiproxy chaos-controller
  gateway-migrations gateway-server gateway-worker
  order-migrations order-service order-worker
  payment-migrations payment-service payment-worker
  inventory-migrations inventory-service inventory-worker
  fulfillment-migrations fulfillment-service fulfillment-worker
  notification-migrations notification-service
)

CHAOS_BUILD_SERVICES=(
  schema-init gateway-migrations gateway-server gateway-worker
  order-migrations order-service order-worker
  payment-migrations payment-service payment-worker
  inventory-migrations inventory-service inventory-worker
  fulfillment-migrations fulfillment-service fulfillment-worker
  notification-migrations notification-service
)

log() { printf '[chaos] %s\n' "$*" >&2; }
uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

compose() { "${COMPOSE[@]}" "$@"; }

# Deixe os predicados de espera nesta biblioteca. O wait_until avalia cada
# predicado num shell novo e com prazo, que faz source do lib.sh; por isso
# funções locais de um cenário não são visíveis ali.
kafka_group_lag() {
  local lag
  lag="$(compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group gateway-dispatcher-chaos --describe 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}')"
  [[ "$lag" == "$1" ]]
}

kafka_group_lag_positive() {
  local lag
  lag="$(compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group gateway-dispatcher-chaos --describe 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}')"
  (( lag > 0 ))
}

kafka_topic_lag_positive() {
  local topic="$1" lag
  lag="$(compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group gateway-dispatcher-chaos --describe 2>/dev/null | awk -v topic="$topic" 'NR>1 && $2 == topic && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}')"
  (( lag > 0 ))
}

reset_order_created_offsets() {
  compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --group gateway-dispatcher-chaos --topic order.created \
    --reset-offsets --to-earliest --execute >/dev/null 2>&1
}

acquire_lock() {
  if ! mkdir "$CHAOS_LOCK" 2>/dev/null; then
    log "another chaos run holds $CHAOS_LOCK"
    return 1
  fi
}

release_lock() { rmdir "$CHAOS_LOCK" 2>/dev/null || true; }

valid_order_id() {
  [[ "$1" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]
}

remember_order() {
  local order_id="$1"
  if ! valid_order_id "$order_id"; then
    log "refusing to persist invalid order id: $order_id"
    return 1
  fi
  CHAOS_LAST_ORDER="$order_id"
  export CHAOS_LAST_ORDER
  printf '%s\n' "$order_id" > "$CHAOS_STATE_FILE"
}

load_last_order() {
  local persisted=""
  [[ -n "${CHAOS_LAST_ORDER:-}" ]] && return 0
  if [[ -r "$CHAOS_STATE_FILE" ]]; then
    IFS= read -r persisted < "$CHAOS_STATE_FILE" || true
    if valid_order_id "$persisted"; then
      CHAOS_LAST_ORDER="$persisted"
      export CHAOS_LAST_ORDER
    else
      log "ignoring invalid chaos state in $CHAOS_STATE_FILE"
    fi
  fi
}

clear_order_state() {
  if [[ -f "$CHAOS_STATE_FILE" ]]; then
    rm -f "$CHAOS_STATE_FILE"
  fi
  CHAOS_LAST_ORDER=""
  export CHAOS_LAST_ORDER
}

wait_until() {
  local timeout="$1" description="$2"
  shift 2
  local deadline remaining
  deadline="$(( $(date +%s) + timeout ))"
  while true; do
    remaining="$(( deadline - $(date +%s) ))"
    if (( remaining <= 0 )); then
      log "timeout after ${timeout}s: $description"
      return 1
    fi

    # As probes rodam num shell novo para o timeout.py poder encerrar o grupo de
    # processos inteiro, inclusive um docker compose exec ou psql travado.
    if python3 "$CHAOS_DIR/timeout.py" "$remaining" -- \
      bash -c 'source "$1"; shift; "$@"' _ "$CHAOS_DIR/lib.sh" "$@"; then
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      log "timeout after ${timeout}s: $description"
      return 1
    fi
    sleep 1
  done
}

http_ok() { curl -fsS --max-time 2 "$1" >/dev/null 2>&1; }

container_healthy() {
  local container_id status
  container_id="$(compose ps -q "$1" 2>/dev/null)"
  [[ -n "$container_id" ]] || return 1
  status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null)"
  [[ "$status" == "healthy" ]]
}

wait_ready() {
  wait_until 180 "chaos controller readiness" http_ok "$CHAOS_CONTROLLER_URL/healthz"
  wait_until 180 "Toxiproxy API readiness" http_ok "$TOXIPROXY_URL/proxies"
  wait_until 180 "Gateway readiness" http_ok "$GATEWAY_URL/healthz"
  wait_until 180 "Order service readiness" http_ok "$ORDER_URL/healthz"
  wait_until 180 "Payment service readiness" http_ok "http://127.0.0.1:$PAYMENT_PORT/healthz"
  wait_until 180 "Inventory service readiness" http_ok "http://127.0.0.1:$INVENTORY_PORT/healthz"
  wait_until 180 "Fulfillment service readiness" http_ok "http://127.0.0.1:$FULFILLMENT_PORT/healthz"
  wait_until 180 "Notification service readiness" http_ok "http://127.0.0.1:$NOTIFICATION_PORT/healthz"
}

stack_up() {
  log "starting disposable stack $CHAOS_PROJECT"
  clear_order_state
  # O Compose v5 delega o build de vários serviços ao Bake, que pode iniciar
  # todos os compiladores Go ao mesmo tempo. O build é serial para manter este
  # gate local estável.
  local service
  if [[ "${CHAOS_SKIP_BUILD:-0}" != "1" ]]; then
    for service in "${CHAOS_BUILD_SERVICES[@]}"; do
      compose build "$service" || return "$?"
    done
  fi
  compose up -d --no-build "${CHAOS_SERVICES[@]}" || return "$?"
  wait_ready || return "$?"
  toxiproxy_reset
  failpoint_reset
}

stack_down() {
  log "removing disposable stack $CHAOS_PROJECT and its volumes"
  local status
  if compose down -v --remove-orphans; then
    clear_order_state
    return 0
  else
    status="$?"
  fi
  log "stack cleanup failed; preserving $CHAOS_STATE_FILE for diagnostics"
  return "$status"
}

sql() {
  local service="$1" user="$2" database="$3" query="$4"
  python3 "$CHAOS_DIR/timeout.py" "$CHAOS_SQL_TIMEOUT_SECONDS" -- \
    "${COMPOSE[@]}" exec -T "$service" \
    env PGCONNECT_TIMEOUT=3 PGOPTIONS='-c statement_timeout=5000' \
    psql -v ON_ERROR_STOP=1 -U "$user" -d "$database" -Atc "$query"
}

gateway_sql() { sql gateway-db gateway gateway_db "$1"; }
order_sql() { sql orders-db orders orders_db "$1"; }
payment_sql() { sql payments-db payments payments_db "$1"; }
inventory_sql() { sql inventory-db inventory inventory_db "$1"; }
fulfillment_sql() { sql fulfillment-db fulfillment fulfillment_db "$1"; }
notification_sql() { sql notifications-db notifications notifications_db "$1"; }

value_is() { [[ "$($1 "$2" 2>/dev/null)" == "$3" ]]; }
value_nonempty() { [[ -n "$($1 "$2" 2>/dev/null)" ]]; }

seed_sku() {
  local sku="$1" available="${2:-10}"
  inventory_sql "INSERT INTO inventory_items(sku,available) VALUES('$sku',$available) ON CONFLICT(sku) DO UPDATE SET available=EXCLUDED.available,updated_at=now()" >/dev/null
}

create_order() {
  local label="$1" sku="$2" amount="${3:-19900}" quantity="${4:-1}"
  local key response order_id
  key="$(uuid)"
  response="$(curl -fsS --max-time 10 -X POST "$ORDER_URL/orders" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"customer_id\":\"chaos-$label\",\"amount_cents\":$amount,\"items\":[{\"sku\":\"$sku\",\"quantity\":$quantity}]}")"
  order_id="$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')"
  remember_order "$order_id"
  printf '%s\n' "$order_id"
}

order_status_is() {
  local order_id="$1" expected="$2"
  value_is order_sql "SELECT status FROM orders WHERE order_id='$order_id'" "$expected"
}

toxiproxy_reset() {
  curl -fsS --max-time 5 -X POST "$TOXIPROXY_URL/reset" >/dev/null 2>&1 || true
}

failpoint_reset() {
  curl -fsS --max-time 5 -X POST "$CHAOS_CONTROLLER_URL/reset" >/dev/null 2>&1 || true
}

arm_failpoint() {
  local rule_id="$1" process="$2" point="$3" match payload
  match="${4:-}"
  [[ -n "$match" ]] || match='{}'
  # Quem monta o body é um parser JSON: interpolar um objeto vazio no shell
  # facilmente gera um argumento válido, mas com uma chave sobrando.
  payload="$(python3 -c 'import json,sys; print(json.dumps({"process":sys.argv[1],"point":sys.argv[2],"match":json.loads(sys.argv[3]),"action":"kill","remaining":1},separators=(",",":")))' "$process" "$point" "$match")"
  curl -fsS --max-time 5 -X PUT "$CHAOS_CONTROLLER_URL/rules/$rule_id" \
    -H 'Content-Type: application/json' \
    -d "$payload" >/dev/null
}

failpoint_hit() {
  curl -fsS --max-time 2 "$CHAOS_CONTROLLER_URL/hits/$1" >/dev/null 2>&1
}

container_exited() {
  local container_id
  container_id="$(compose ps -q --all "$1" 2>/dev/null)"
  [[ -n "$container_id" ]] || return 1
  [[ "$(docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null)" == "false" ]]
}

wait_crash() {
  local rule_id="$1" service="$2"
  wait_until 15 "failpoint $rule_id is hit" failpoint_hit "$rule_id"
  wait_until 15 "$service exits after failpoint" container_exited "$service"
}

start_service() {
  compose start "$1" >/dev/null
}

proxy_enabled() {
  local name="$1" enabled="$2"
  curl -fsS --max-time 5 -X POST "$TOXIPROXY_URL/proxies/$name" \
    -H 'Content-Type: application/json' -d "{\"enabled\":$enabled}" >/dev/null
}

add_limit_data_toxic() {
  local proxy="$1" name="$2"
  curl -fsS --max-time 5 -X POST "$TOXIPROXY_URL/proxies/$proxy/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"type\":\"limit_data\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"bytes\":0}}" >/dev/null
}

remove_toxic() {
  curl -fsS --max-time 5 -X DELETE "$TOXIPROXY_URL/proxies/$1/toxics/$2" >/dev/null 2>&1 || true
}

expect_http_status() {
  local expected="$1" url="$2" actual
  actual="$(curl -sS --max-time 2 -o /dev/null -w '%{http_code}' "$url" || true)"
  [[ "$actual" == "$expected" ]]
}

diagnostics() {
  if [[ -n "${1:-}" ]]; then
    remember_order "$1" || return 2
  else
    load_last_order
  fi
  log "collecting diagnostics (last order: ${CHAOS_LAST_ORDER:-unknown})"
  compose ps || true
  compose logs --tail=80 chaos-controller gateway-server gateway-worker order-service order-worker payment-service payment-worker inventory-service inventory-worker fulfillment-service fulfillment-worker notification-service || true
  curl -fsS --max-time 3 "$CHAOS_CONTROLLER_URL/state" || true
  compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group gateway-dispatcher-chaos --describe || true
  if [[ -n "${CHAOS_LAST_ORDER:-}" ]]; then
    order_sql "SELECT order_id,status,cancel_reason FROM orders WHERE order_id='$CHAOS_LAST_ORDER'" || true
    order_sql "SELECT event_type,status,retry_count,last_error FROM outbox WHERE correlation_id='$CHAOS_LAST_ORDER' ORDER BY created_at" || true
    gateway_sql "SELECT event_type,status,retry_count,last_error FROM outbox WHERE correlation_id='$CHAOS_LAST_ORDER' ORDER BY created_at" || true
    gateway_sql "SELECT event_type,service_id,status,retry_count,last_error FROM dispatch_outbox WHERE correlation_id='$CHAOS_LAST_ORDER' ORDER BY created_at" || true
    payment_sql "SELECT order_id,status FROM payments WHERE order_id='$CHAOS_LAST_ORDER'" || true
    inventory_sql "SELECT order_id,status FROM reservations WHERE order_id='$CHAOS_LAST_ORDER'" || true
    fulfillment_sql "SELECT order_id,payment_id,reservation_id,fulfillment_id FROM fulfillment_state WHERE order_id='$CHAOS_LAST_ORDER'" || true
    notification_sql "SELECT event_type,order_id FROM notifications WHERE order_id='$CHAOS_LAST_ORDER'" || true
  fi
}

recover_faults() {
  toxiproxy_reset
  failpoint_reset
  compose unpause kafka >/dev/null 2>&1 || true
  compose start schema-registry gateway-db orders-db payments-db inventory-db fulfillment-db notifications-db \
    gateway-server gateway-worker order-service order-worker payment-service payment-worker \
    inventory-service inventory-worker fulfillment-service fulfillment-worker notification-service >/dev/null 2>&1 || true
}

assert_equals() {
  local expected="$1" actual="$2" description="$3"
  if [[ "$actual" != "$expected" ]]; then
    log "$description: expected '$expected', got '$actual'"
    return 1
  fi
}
