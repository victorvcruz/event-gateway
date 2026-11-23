#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/infrastructure/docker-compose.yml")
DISPATCH_TOKEN="${DISPATCH_TOKEN:-local-dev-dispatch-token}"
uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }
now() { python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())'; }
post_dispatch() {
  local url="$1" body="$2" correlation="$3"
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$url/dispatch" -H 'Content-Type: application/json' -H "X-Correlation-Id: $correlation" -H "X-Gateway-Token: $DISPATCH_TOKEN" -d "$body")"
  [[ "$code" == "200" || "$code" == "409" ]]
}

for port in 8082 8083; do
  for _ in $(seq 1 60); do
    curl -fsS "http://localhost:$port/healthz" >/dev/null && break
    sleep 1
  done
  curl -fsS "http://localhost:$port/healthz" >/dev/null
done

inventory_order="$(uuid)"; inventory_corr="$inventory_order"
post_dispatch http://localhost:8083 "{\"event_id\":\"$(uuid)\",\"event_type\":\"payment.failed\",\"correlation_id\":\"$inventory_corr\",\"timestamp\":\"$(now)\",\"payload\":{\"payment_id\":\"$(uuid)\",\"order_id\":\"$inventory_order\",\"reason\":\"approval_limit_exceeded\"}}" "$inventory_corr"
post_dispatch http://localhost:8083 "{\"event_id\":\"$(uuid)\",\"event_type\":\"order.created\",\"correlation_id\":\"$inventory_corr\",\"timestamp\":\"$(now)\",\"payload\":{\"order_id\":\"$inventory_order\",\"customer_id\":\"out-of-order\",\"amount_cents\":100,\"items\":[{\"sku\":\"SKU-001\",\"quantity\":1}]}}" "$inventory_corr"
inventory_status="$(${COMPOSE[@]} exec -T inventory-db psql -U inventory -d inventory_db -Atc "SELECT status FROM reservations WHERE order_id='$inventory_order'")"
[[ "$inventory_status" == "SKIPPED" ]]

payment_order="$(uuid)"; payment_corr="$payment_order"
post_dispatch http://localhost:8082 "{\"event_id\":\"$(uuid)\",\"event_type\":\"stock.failed\",\"correlation_id\":\"$payment_corr\",\"timestamp\":\"$(now)\",\"payload\":{\"order_id\":\"$payment_order\",\"reason\":\"sku_not_found\",\"unavailable_skus\":[\"UNKNOWN\"]}}" "$payment_corr"
post_dispatch http://localhost:8082 "{\"event_id\":\"$(uuid)\",\"event_type\":\"order.created\",\"correlation_id\":\"$payment_corr\",\"timestamp\":\"$(now)\",\"payload\":{\"order_id\":\"$payment_order\",\"customer_id\":\"out-of-order\",\"amount_cents\":100,\"items\":[{\"sku\":\"SKU-001\",\"quantity\":1}]}}" "$payment_corr"
payment_status="$(${COMPOSE[@]} exec -T payments-db psql -U payments -d payments_db -Atc "SELECT status FROM payments WHERE order_id='$payment_order'")"
[[ "$payment_status" == "SKIPPED" ]]

printf 'out-of-order compensation markers honored: inventory=%s payment=%s\n' "$inventory_status" "$payment_status"
