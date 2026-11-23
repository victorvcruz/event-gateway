#!/usr/bin/env bash
set -euo pipefail

post_order_expect_disconnect() {
  local key="$1" sku="$2" label="$3" amount="${4:-19900}"
  local status
  set +e
  curl -sS --max-time 10 -X POST "$ORDER_URL/orders" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "{\"customer_id\":\"chaos-$label\",\"amount_cents\":$amount,\"items\":[{\"sku\":\"$sku\",\"quantity\":1}]}" >/dev/null
  status="$?"
  set -e
  (( status != 0 ))
}

post_event_expect_disconnect() {
  local body="$1" status
  set +e
  curl -sS --max-time 10 -X POST "$GATEWAY_URL/events" -H 'Content-Type: application/json' -d "$body" >/dev/null
  status="$?"
  set -e
  (( status != 0 ))
}

scenario_order_http_after_commit() {
  log "scenario: Order commit survives lost HTTP response"
  local sku="CRASH-ORDER-${RANDOM}-$(date +%s)" key order_id response
  seed_sku "$sku"
  compose stop order-worker >/dev/null
  key="$(uuid)"
  arm_failpoint order-http order-service order.http.after_commit '{}'
  post_order_expect_disconnect "$key" "$sku" order-http
  wait_crash order-http order-service
  order_id="$(order_sql "SELECT order_id FROM orders WHERE idempotency_key='$key'")"
  remember_order "$order_id"
  assert_equals 1 "$(order_sql "SELECT count(*) FROM orders WHERE idempotency_key='$key'")" "committed order cardinality"
  assert_equals 1 "$(order_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "atomic order outbox"
  start_service order-service
  wait_until 30 "Order service restart" http_ok "$ORDER_URL/healthz"
  response="$(curl -fsS --max-time 10 -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" -d "{\"customer_id\":\"chaos-order-http\",\"amount_cents\":19900,\"items\":[{\"sku\":\"$sku\",\"quantity\":1}]}")"
  assert_equals "$order_id" "$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')" "idempotent retry order id"
  start_service order-worker
  wait_until 90 "order HTTP crash converges" order_status_is "$order_id" FULFILLED
}

scenario_gateway_ingest_after_commit() {
  log "scenario: Gateway ingest commit survives lost 202"
  local event_id order_id timestamp body status
  event_id="$(uuid)"; order_id="$(uuid)"
  remember_order "$order_id"
  timestamp="$(python3 -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))')"
  body="{\"event_id\":\"$event_id\",\"event_type\":\"order.cancelled\",\"correlation_id\":\"$order_id\",\"timestamp\":\"$timestamp\",\"payload\":{\"order_id\":\"$order_id\",\"reason\":\"customer_request\"}}"
  compose stop gateway-worker >/dev/null
  arm_failpoint gateway-ingest gateway-server gateway.http.after_ingest_commit "{\"correlation_id\":\"$order_id\"}"
  post_event_expect_disconnect "$body"
  wait_crash gateway-ingest gateway-server
  assert_equals 1 "$(gateway_sql "SELECT count(*) FROM outbox WHERE event_id='$event_id' AND status='PENDING'")" "Gateway outbox commit"
  assert_equals 1 "$(gateway_sql "SELECT count(*) FROM event_log WHERE event_id='$event_id' AND direction='RECEIVED'")" "Gateway event log commit"
  start_service gateway-server
  wait_until 30 "Gateway server restart" http_ok "$GATEWAY_URL/healthz"
  status="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' -X POST "$GATEWAY_URL/events" -H 'Content-Type: application/json' -d "$body")"
  assert_equals 409 "$status" "duplicate ingest response"
  start_service gateway-worker
  wait_until 90 "persisted event reaches notification" value_is notification_sql "SELECT count(*) FROM notifications WHERE event_id='$event_id'" 1
  assert_equals SENT "$(gateway_sql "SELECT status FROM outbox WHERE event_id='$event_id'")" "Gateway outbox drained"
}

scenario_dispatch_after_business_commit() {
  log "scenario: subscriber crashes after business commit"
  local sku="CRASH-DISPATCH-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku" 10
  compose stop order-worker >/dev/null
  order_id="$(create_order dispatch-commit "$sku")"
  arm_failpoint dispatch-commit inventory-service dispatch.after_handler_commit "{\"correlation_id\":\"$order_id\",\"event_type\":\"order.created\"}"
  start_service order-worker
  wait_crash dispatch-commit inventory-service
  assert_equals ACTIVE "$(inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'")" "reservation committed"
  assert_equals 9 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock decremented once before crash"
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='stock.reserved'")" "stock event committed once"
  start_service inventory-service
  wait_until 30 "Inventory restart" http_ok "http://127.0.0.1:$INVENTORY_PORT/healthz"
  wait_until 90 "dispatch retry converges" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM reservations WHERE order_id='$order_id'")" "reservation remains unique"
  assert_equals 9 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock remains decremented once"
}

scenario_worker_after_gateway_ack() {
  log "scenario: business worker crashes after Gateway acknowledgement"
  local sku="CRASH-WACK-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop order-worker >/dev/null
  order_id="$(create_order worker-ack "$sku")"
  arm_failpoint worker-ack order-worker outbox.after_send_ack "{\"correlation_id\":\"$order_id\",\"outbox_table\":\"outbox\"}"
  start_service order-worker
  wait_crash worker-ack order-worker
  assert_equals PENDING "$(order_sql "SELECT status FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "local outbox remains pending"
  assert_equals 0 "$(order_sql "SELECT retry_count FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "ACK crash is not counted as send failure"
  assert_equals 1 "$(gateway_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "Gateway accepted once"
  start_service order-worker
  wait_until 90 "worker ACK retry converges" order_status_is "$order_id" FULFILLED
  # A saga pode chegar a FULFILLED enquanto a transação do order-worker em retry
  # ainda está entre o ACK do 409 duplicado e o COMMIT local. Por isso as duas
  # condições de convergência são observadas separadamente, sem assumir ordem
  # entre dois bancos.
  wait_until 30 "local outbox is committed as sent" value_is order_sql "SELECT status FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'" SENT
}

scenario_kafka_producer_after_ack() {
  log "scenario: Gateway relay crashes after broker ACK"
  local sku="CRASH-KACK-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop order-worker >/dev/null
  order_id="$(create_order kafka-ack "$sku")"
  arm_failpoint kafka-ack gateway-worker outbox.after_send_ack "{\"correlation_id\":\"$order_id\",\"outbox_table\":\"outbox\"}"
  start_service order-worker
  wait_crash kafka-ack gateway-worker
  assert_equals PENDING "$(gateway_sql "SELECT status FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "Gateway relay outbox after ACK crash"
  start_service gateway-worker
  wait_until 120 "Kafka duplicate converges" order_status_is "$order_id" FULFILLED
  assert_equals 2 "$(gateway_sql "SELECT count(*) FROM dispatch_outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "fanout cardinality after Kafka duplicate"
  assert_equals 1 "$(payment_sql "SELECT count(*) FROM payments WHERE order_id='$order_id'")" "payment cardinality"
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM reservations WHERE order_id='$order_id'")" "reservation cardinality"
}

scenario_kafka_consumer_before_offset() {
  log "scenario: dispatch rows commit before Kafka offset"
  local sku="CRASH-KOFFSET-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop order-worker >/dev/null
  order_id="$(create_order kafka-offset "$sku")"
  arm_failpoint kafka-offset gateway-worker kafka.consumer.after_db_commit "{\"correlation_id\":\"$order_id\",\"event_type\":\"order.created\"}"
  start_service order-worker
  wait_crash kafka-offset gateway-worker
  assert_equals 2 "$(gateway_sql "SELECT count(*) FROM dispatch_outbox WHERE correlation_id='$order_id' AND event_type='order.created' AND status='PENDING'")" "dispatch rows committed before offset"
  wait_until 20 "consumer group retains lag" kafka_group_lag_positive
  start_service gateway-worker
  wait_until 120 "consumer replay converges" order_status_is "$order_id" FULFILLED
  assert_equals 2 "$(gateway_sql "SELECT count(*) FROM dispatch_outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "consumer replay is idempotent"
}

scenario_kafka_offset_rewind() {
  log "scenario: real Kafka consumer offset rewind"
  local sku="CRASH-REWIND-${RANDOM}-$(date +%s)" order_id before_dispatch before_reservations before_payments
  seed_sku "$sku"
  order_id="$(create_order kafka-rewind "$sku")"
  wait_until 90 "rewind baseline saga" order_status_is "$order_id" FULFILLED
  before_dispatch="$(gateway_sql 'SELECT count(*) FROM dispatch_outbox')"
  before_reservations="$(inventory_sql 'SELECT count(*) FROM reservations')"
  before_payments="$(payment_sql 'SELECT count(*) FROM payments')"
  compose stop gateway-worker >/dev/null
  wait_until 30 "consumer group becomes inactive and offset rewinds" reset_order_created_offsets
  wait_until 20 "offset rewind creates positive order.created lag" kafka_topic_lag_positive order.created
  start_service gateway-worker
  wait_until 120 "rewound records are consumed" kafka_group_lag 0
  assert_equals "$before_dispatch" "$(gateway_sql 'SELECT count(*) FROM dispatch_outbox')" "dispatch cardinality after rewind"
  assert_equals "$before_reservations" "$(inventory_sql 'SELECT count(*) FROM reservations')" "reservation cardinality after rewind"
  assert_equals "$before_payments" "$(payment_sql 'SELECT count(*) FROM payments')" "payment cardinality after rewind"
}

scenario_payment_compensation_crash() {
  log "scenario: payment compensation rolls back on crash"
  local sku="MISSING-${RANDOM}-$(date +%s)" order_id
  proxy_enabled inventory_callback false
  order_id="$(create_order payment-compensation "$sku")"
  wait_until 60 "payment is processed before stock failure" value_is payment_sql "SELECT status FROM payments WHERE order_id='$order_id'" PROCESSED
  arm_failpoint payment-comp payment-service payment.compensation.before_commit "{\"correlation_id\":\"$order_id\"}"
  proxy_enabled inventory_callback true
  wait_crash payment-comp payment-service
  assert_equals PROCESSED "$(payment_sql "SELECT status FROM payments WHERE order_id='$order_id'")" "payment compensation rollback"
  assert_equals 0 "$(payment_sql "SELECT count(*) FROM refund_requests WHERE order_id='$order_id'")" "refund request rolled back"
  assert_equals 0 "$(payment_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='payment.refunded'")" "refund event rolled back"
  start_service payment-service
  wait_until 30 "Payment restart" http_ok "http://127.0.0.1:$PAYMENT_PORT/healthz"
  wait_until 90 "payment compensation retries" value_is payment_sql "SELECT status FROM payments WHERE order_id='$order_id'" REFUNDED
  wait_until 90 "stock failure cancels order" order_status_is "$order_id" CANCELLED
  assert_equals 1 "$(payment_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='payment.refunded'")" "one refund event"
}

scenario_inventory_compensation_crash() {
  log "scenario: inventory compensation rolls back on crash"
  local sku="CRASH-ICOMP-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku" 1
  proxy_enabled payment_callback false
  order_id="$(create_order inventory-compensation "$sku" 100001)"
  wait_until 60 "inventory reserves before payment failure" value_is inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'" ACTIVE
  arm_failpoint inventory-comp inventory-service inventory.compensation.before_commit "{\"correlation_id\":\"$order_id\"}"
  proxy_enabled payment_callback true
  wait_crash inventory-comp inventory-service
  assert_equals ACTIVE "$(inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'")" "inventory compensation rollback"
  assert_equals 0 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock remains reserved after rollback"
  assert_equals 0 "$(inventory_sql "SELECT count(*) FROM release_requests WHERE order_id='$order_id'")" "release request rolled back"
  start_service inventory-service
  wait_until 30 "Inventory restart" http_ok "http://127.0.0.1:$INVENTORY_PORT/healthz"
  wait_until 90 "inventory compensation retries" value_is inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'" RELEASED
  wait_until 90 "payment failure cancels order" order_status_is "$order_id" CANCELLED
  assert_equals 1 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock restored once"
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='stock.released'")" "one release event"
}

scenario_same_sku_contention() {
  log "scenario: ten full-stack orders contend for one SKU"
  local sku="CRASH-CONTENTION-${RANDOM}-$(date +%s)" label ids first_id quoted
  sku="${sku:0:250}"
  label="contention-${RANDOM}-$(date +%s)"
  seed_sku "$sku" 1
  ids="$(python3 "$CHAOS_DIR/contention.py" --url "$ORDER_URL" --sku "$sku" --label "$label" --count 10)"
  first_id="$(printf '%s\n' "$ids" | head -1)"
  remember_order "$first_id"
  wait_until 120 "all contending orders become terminal" value_is order_sql "SELECT count(*) FROM orders WHERE customer_id LIKE 'chaos-$label-%' AND status IN ('FULFILLED','CANCELLED')" 10
  assert_equals 1 "$(order_sql "SELECT count(*) FROM orders WHERE customer_id LIKE 'chaos-$label-%' AND status='FULFILLED'")" "one contention winner"
  assert_equals 9 "$(order_sql "SELECT count(*) FROM orders WHERE customer_id LIKE 'chaos-$label-%' AND status='CANCELLED' AND cancel_reason='stock_failed'")" "nine contention losers"
  assert_equals 0 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "contention stock balance"
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM reservations WHERE items @> '[{\"sku\":\"$sku\"}]'::jsonb AND status='ACTIVE'")" "one active reservation"
  assert_equals 9 "$(inventory_sql "SELECT count(*) FROM reservations WHERE items @> '[{\"sku\":\"$sku\"}]'::jsonb AND status='FAILED'")" "nine failed reservations"
  quoted="$(order_sql "SELECT string_agg(quote_literal(order_id::text),',') FROM orders WHERE customer_id LIKE 'chaos-$label-%'")"
  wait_until 120 "loser payments are reconciled" value_is payment_sql "SELECT count(*) FROM payments WHERE order_id IN ($quoted) AND status IN ('REFUNDED','SKIPPED')" 9
  wait_until 120 "terminal notifications are delivered" value_is notification_sql "SELECT count(*) FROM notifications WHERE order_id IN ($quoted) AND event_type IN ('order.fulfilled','order.cancelled')" 10
  assert_equals 1 "$(payment_sql "SELECT count(*) FROM payments WHERE order_id IN ($quoted) AND status='PROCESSED'")" "winner payment remains processed"
  assert_equals 9 "$(payment_sql "SELECT count(*) FROM payments WHERE order_id IN ($quoted) AND status IN ('REFUNDED','SKIPPED')")" "loser payments reconciled"
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id IN ($quoted) AND fulfillment_id IS NOT NULL")" "one fulfillment"
  assert_equals 1 "$(notification_sql "SELECT count(*) FROM notifications WHERE order_id IN ($quoted) AND event_type='order.fulfilled'")" "one fulfilled notification"
  assert_equals 9 "$(notification_sql "SELECT count(*) FROM notifications WHERE order_id IN ($quoted) AND event_type='order.cancelled'")" "nine cancellation notifications"
}

run_crash_scenario() {
  case "$1" in
    order-http-after-commit) scenario_order_http_after_commit ;;
    gateway-ingest-after-commit) scenario_gateway_ingest_after_commit ;;
    dispatch-after-business-commit) scenario_dispatch_after_business_commit ;;
    worker-after-gateway-ack) scenario_worker_after_gateway_ack ;;
    kafka-producer-after-ack) scenario_kafka_producer_after_ack ;;
    kafka-consumer-before-offset) scenario_kafka_consumer_before_offset ;;
    kafka-offset-rewind) scenario_kafka_offset_rewind ;;
    payment-compensation-crash) scenario_payment_compensation_crash ;;
    inventory-compensation-crash) scenario_inventory_compensation_crash ;;
    same-sku-contention) scenario_same_sku_contention ;;
    *) return 2 ;;
  esac
}
