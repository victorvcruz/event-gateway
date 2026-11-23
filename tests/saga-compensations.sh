#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/infrastructure/docker-compose.yml")
ORDER_URL="${ORDER_URL:-http://localhost:8081}"

for _ in $(seq 1 60); do
  curl -fsS "$ORDER_URL/healthz" >/dev/null && break
  sleep 1
done
curl -fsS "$ORDER_URL/healthz" >/dev/null

uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

create_order() {
  local key="$1" amount="$2" sku="$3"
  curl -fsS -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $key" \
    -d "{\"customer_id\":\"compensation-smoke\",\"amount_cents\":$amount,\"items\":[{\"sku\":\"$sku\",\"quantity\":1}]}"
}

wait_cancelled() {
  local order_id="$1"
  for _ in $(seq 1 60); do
    local body status
    body="$(curl -fsS "$ORDER_URL/orders/$order_id")"
    status="$(printf '%s' "$body" | json_field status)"
    [[ "$status" == "CANCELLED" ]] && return 0
    [[ "$status" == "FULFILLED" ]] && { echo "unexpected fulfillment: $body" >&2; return 1; }
    sleep 1
  done
  return 1
}

payment_key="$(uuid)"
payment_response="$(create_order "$payment_key" 100001 SKU-001)"
payment_order="$(printf '%s' "$payment_response" | json_field order_id)"
duplicate_order="$(create_order "$payment_key" 100001 SKU-001 | json_field order_id)"
[[ "$payment_order" == "$duplicate_order" ]]
wait_cancelled "$payment_order"
inventory_status="$(${COMPOSE[@]} exec -T inventory-db psql -U inventory -d inventory_db -Atc "SELECT status FROM reservations WHERE order_id='$payment_order'")"
[[ "$inventory_status" == "RELEASED" || "$inventory_status" == "SKIPPED" ]]

stock_key="$(uuid)"
stock_response="$(create_order "$stock_key" 100 UNKNOWN-SKU)"
stock_order="$(printf '%s' "$stock_response" | json_field order_id)"
wait_cancelled "$stock_order"
payment_status="$(${COMPOSE[@]} exec -T payments-db psql -U payments -d payments_db -Atc "SELECT status FROM payments WHERE order_id='$stock_order'")"
[[ "$payment_status" == "REFUNDED" || "$payment_status" == "SKIPPED" ]]

printf 'payment failure compensated: %s (%s)\n' "$payment_order" "$inventory_status"
printf 'stock failure compensated: %s (%s)\n' "$stock_order" "$payment_status"
