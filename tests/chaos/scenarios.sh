#!/usr/bin/env bash
set -euo pipefail

scenario_gateway_outage() {
  log "scenario: business worker -> Gateway outage"
  local sku="CHAOS-GW-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  proxy_enabled gateway_http false
  order_id="$(create_order gateway-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 15 "order outbox records a retry" value_nonempty order_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND status='PENDING' AND retry_count>=1 AND last_error IS NOT NULL"
  assert_equals CREATED "$(order_sql "SELECT status FROM orders WHERE order_id='$order_id'")" "order state during outage"
  proxy_enabled gateway_http true
  wait_until 90 "saga recovers after Gateway returns" order_status_is "$order_id" FULFILLED
  assert_equals SENT "$(order_sql "SELECT status FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "order outbox recovery"
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id='$order_id' AND fulfillment_id IS NOT NULL")" "fulfillment cardinality"
}

scenario_lost_ack() {
  log "scenario: subscriber commits but its acknowledgement is lost"
  local sku="CHAOS-ACK-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku" 10
  add_limit_data_toxic inventory_callback lost_ack
  order_id="$(create_order lost-ack "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 30 "inventory commit followed by dispatch retry" value_nonempty gateway_sql \
    "SELECT 1 FROM dispatch_outbox WHERE correlation_id='$order_id' AND service_id='inventory-service' AND retry_count>=1"
  assert_equals ACTIVE "$(inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'")" "reservation committed before ACK"
  remove_toxic inventory_callback lost_ack
  wait_until 90 "lost ACK retry converges" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM reservations WHERE order_id='$order_id'")" "reservation cardinality"
  assert_equals 9 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock decremented once"
  assert_equals 1 "$(inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='stock.reserved'")" "stock event cardinality"
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM dead_letter WHERE event_id IN (SELECT event_id FROM dispatch_outbox WHERE correlation_id='$order_id')")" "lost ACK dead letters"
}

scenario_fulfillment_outage() {
  log "scenario: Fulfillment subscriber unavailable"
  local sku="CHAOS-FUL-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  proxy_enabled fulfillment_callback false
  order_id="$(create_order fulfillment-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 30 "both fulfillment inputs remain retryable" value_is gateway_sql \
    "SELECT count(*) FROM dispatch_outbox WHERE correlation_id='$order_id' AND service_id='fulfillment-service' AND status='PENDING' AND retry_count>=1" 2
  proxy_enabled fulfillment_callback true
  wait_until 90 "fulfillment recovers" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id='$order_id' AND fulfillment_id IS NOT NULL")" "fulfillment cardinality"
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='order.fulfilled'")" "fulfilled event cardinality"
}

scenario_worker_restart() {
  log "scenario: business outbox survives worker restart"
  local sku="CHAOS-OW-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop order-worker >/dev/null
  order_id="$(create_order order-worker-restart "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  assert_equals PENDING "$(order_sql "SELECT status FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "outbox while worker is stopped"
  assert_equals CREATED "$(order_sql "SELECT status FROM orders WHERE order_id='$order_id'")" "domain commit while worker is stopped"
  compose start order-worker >/dev/null
  wait_until 90 "order worker restart drains outbox" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(order_sql "SELECT count(*) FROM orders WHERE order_id='$order_id'")" "order cardinality"
  assert_equals 1 "$(order_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "order.created cardinality"
}

scenario_gateway_worker_restart() {
  log "scenario: Gateway outbox survives worker restart"
  local sku="CHAOS-GWW-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop gateway-worker >/dev/null
  order_id="$(create_order gateway-worker-restart "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 15 "Gateway accepts event while worker is stopped" value_nonempty gateway_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created' AND status='PENDING'"
  compose start gateway-worker >/dev/null
  wait_until 90 "Gateway worker restart drains outbox" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(gateway_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created'")" "Gateway event cardinality"
}

scenario_kafka_outage() {
  log "scenario: Kafka pause and recovery"
  local sku="CHAOS-KAFKA-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose pause kafka >/dev/null
  order_id="$(create_order kafka-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 20 "Gateway outbox persists while Kafka is paused" value_nonempty gateway_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND event_type='order.created' AND status='PENDING'"
  compose unpause kafka >/dev/null
  wait_until 60 "Kafka health recovers after unpause" container_healthy kafka
  wait_until 120 "saga recovers after Kafka resumes" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id='$order_id' AND fulfillment_id IS NOT NULL")" "post-Kafka fulfillment cardinality"
}

scenario_schema_registry_outage() {
  log "scenario: Schema Registry fail-closed and recovery"
  local sku="CHAOS-SR-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop schema-registry >/dev/null
  wait_until 10 "Gateway health returns 503 without Schema Registry" expect_http_status 503 "$GATEWAY_URL/healthz"
  # O perfil de chaos usa de propósito um TTL de 2s no cache de schemas. Espera
  # até nenhuma entrada em cache poder ser usada antes de criar o evento.
  sleep 3
  order_id="$(create_order schema-registry-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 20 "business outbox retries after fail-closed ingest" value_nonempty order_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND status='PENDING' AND retry_count>=1 AND last_error IS NOT NULL"
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id'")" "Gateway rejects event while Registry is unavailable"
  compose start schema-registry >/dev/null
  wait_until 30 "Gateway readiness recovers with Schema Registry" http_ok "$GATEWAY_URL/healthz"
  wait_until 90 "Registry recovery drains the event backlog" order_status_is "$order_id" FULFILLED
  assert_equals 1 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id='$order_id' AND fulfillment_id IS NOT NULL")" "post-Registry fulfillment cardinality"
}

scenario_database_outage() {
  log "scenario: Payment database outage and readiness"
  local sku="CHAOS-PDB-${RANDOM}-$(date +%s)" order_id
  seed_sku "$sku"
  compose stop payments-db >/dev/null
  wait_until 10 "Payment health returns 503" expect_http_status 503 "http://127.0.0.1:$PAYMENT_PORT/healthz"
  order_id="$(create_order payment-db-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 20 "Payment dispatch is retryable" value_nonempty gateway_sql \
    "SELECT 1 FROM dispatch_outbox WHERE correlation_id='$order_id' AND service_id='payment-service' AND status='PENDING' AND retry_count>=1"
  compose start payments-db >/dev/null
  wait_until 30 "Payment health recovers" http_ok "http://127.0.0.1:$PAYMENT_PORT/healthz"
  wait_until 90 "saga recovers after Payment DB" order_status_is "$order_id" FULFILLED

  log "scenario: Gateway database outage and readiness"
  sku="CHAOS-GDB-${RANDOM}-$(date +%s)"
  seed_sku "$sku"
  compose stop gateway-db >/dev/null
  wait_until 10 "Gateway health returns 503" expect_http_status 503 "$GATEWAY_URL/healthz"
  order_id="$(create_order gateway-db-outage "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 20 "business outbox retries while Gateway DB is unavailable" value_nonempty order_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND status='PENDING' AND retry_count>=1"
  compose start gateway-db >/dev/null
  wait_until 30 "Gateway health recovers" http_ok "$GATEWAY_URL/healthz"
  wait_until 90 "saga recovers after Gateway DB" order_status_is "$order_id" FULFILLED
}

scenario_dead_letter() {
  log "scenario: subscriber retry exhaustion and dead letter"
  local sku="CHAOS-DLQ-${RANDOM}-$(date +%s)" order_id event_id
  seed_sku "$sku"
  proxy_enabled notification_callback false
  order_id="$(create_order dead-letter "$sku")"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 90 "order reaches FULFILLED despite notification outage" order_status_is "$order_id" FULFILLED
  wait_until 45 "notification dispatch reaches DEAD" value_nonempty gateway_sql \
    "SELECT 1 FROM dispatch_outbox WHERE correlation_id='$order_id' AND event_type='order.fulfilled' AND service_id='notification-service' AND status='DEAD' AND retry_count=6 AND last_error IS NOT NULL"
  event_id="$(gateway_sql "SELECT event_id FROM dispatch_outbox WHERE correlation_id='$order_id' AND event_type='order.fulfilled' AND service_id='notification-service'")"
  assert_equals 1 "$(gateway_sql "SELECT count(*) FROM dead_letter WHERE event_id='$event_id' AND service_id='notification-service'")" "dead-letter cardinality"
  assert_equals 0 "$(notification_sql "SELECT count(*) FROM notifications WHERE order_id='$order_id' AND event_type='order.fulfilled'")" "notification absence"
}

run_scenario() {
  case "$1" in
    gateway-outage) scenario_gateway_outage ;;
    lost-ack) scenario_lost_ack ;;
    fulfillment-outage) scenario_fulfillment_outage ;;
    worker-restart) scenario_worker_restart ;;
    gateway-worker-restart) scenario_gateway_worker_restart ;;
    kafka-outage) scenario_kafka_outage ;;
    schema-registry-outage) scenario_schema_registry_outage ;;
    database-outage) scenario_database_outage ;;
    dead-letter) scenario_dead_letter ;;
    *) log "unknown scenario: $1"; return 2 ;;
  esac
}
