#!/usr/bin/env bash
set -euo pipefail

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
KEY="$(python3 -c 'import uuid; print(uuid.uuid4())')"

for _ in $(seq 1 60); do
  curl -fsS "${ORDER_URL}/healthz" >/dev/null && break
  sleep 1
done
curl -fsS "${ORDER_URL}/healthz" >/dev/null

response="$(curl -fsS -X POST "${ORDER_URL}/orders" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${KEY}" \
  -d '{"customer_id":"smoke-customer","amount_cents":19900,"items":[{"sku":"SKU-001","quantity":1}]}')"
order_id="$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')"

for _ in $(seq 1 60); do
  current="$(curl -fsS "${ORDER_URL}/orders/${order_id}")"
  status="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$status" == "FULFILLED" ]]; then
    printf 'saga fulfilled: %s\n' "$order_id"
    exit 0
  fi
  if [[ "$status" == "CANCELLED" ]]; then
    printf 'saga cancelled unexpectedly: %s\n' "$current" >&2
    exit 1
  fi
  sleep 1
done

printf 'timeout waiting for saga: %s\n' "$order_id" >&2
exit 1
