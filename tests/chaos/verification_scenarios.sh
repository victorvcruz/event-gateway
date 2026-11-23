#!/usr/bin/env bash
set -euo pipefail
# Cenários de verificação acrescentados para o TCC (Fase B):
#   invalid-event-not-dispatched     — T2: evento fora do contrato recebe 422 e
#                                      não é persistido, publicado nem despachado.
#   compensation-during-gateway-outage — T8: falha de infraestrutura combinada
#                                      com caminho de compensação de negócio.

scenario_invalid_event_not_dispatched() {
  log "scenario: schema-invalid event is rejected and never reaches subscribers"
  local event_id order_id timestamp body status
  event_id="$(uuid)"; order_id="$(uuid)"
  remember_order "$order_id"
  timestamp="$(python3 -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))')"
  # amount_cents deve ser inteiro; enviamos string, violando o JSON Schema de order.created.
  body="{\"event_id\":\"$event_id\",\"event_type\":\"order.created\",\"correlation_id\":\"$order_id\",\"timestamp\":\"$timestamp\",\"payload\":{\"order_id\":\"$order_id\",\"customer_id\":\"chaos-invalid\",\"amount_cents\":\"cem\",\"items\":[{\"sku\":\"SKU-001\",\"quantity\":1}]}}"
  status="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' -X POST "$GATEWAY_URL/events" -H 'Content-Type: application/json' -d "$body")"
  assert_equals 422 "$status" "schema violation response"
  # Aguarda um ciclo completo de relay e despacho antes de verificar a ausência.
  sleep 5
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM outbox WHERE event_id='$event_id'")" "invalid event not persisted in Gateway outbox"
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM event_log WHERE event_id='$event_id'")" "invalid event not logged"
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM dispatch_outbox WHERE event_id='$event_id'")" "invalid event not dispatched"
  assert_equals 0 "$(payment_sql "SELECT count(*) FROM payments WHERE order_id='$order_id'")" "payment subscriber untouched"
  assert_equals 0 "$(inventory_sql "SELECT count(*) FROM reservations WHERE order_id='$order_id'")" "inventory subscriber untouched"
}

scenario_compensation_during_gateway_outage() {
  log "scenario: payment compensation path while the Gateway is unavailable"
  local sku="CHAOS-CMP-${RANDOM}-$(date +%s)" order_id reservation
  seed_sku "$sku" 10
  proxy_enabled gateway_http false
  # Valor acima do limite de aprovação (100000) força payment.failed.
  order_id="$(create_order compensation-outage "$sku" 100001)"
  CHAOS_LAST_ORDER="$order_id"
  wait_until 15 "order outbox records a retry during outage" value_nonempty order_sql \
    "SELECT 1 FROM outbox WHERE correlation_id='$order_id' AND status='PENDING' AND retry_count>=1 AND last_error IS NOT NULL"
  assert_equals CREATED "$(order_sql "SELECT status FROM orders WHERE order_id='$order_id'")" "order state during outage"
  proxy_enabled gateway_http true
  wait_until 120 "compensation converges after Gateway returns" order_status_is "$order_id" CANCELLED
  assert_equals FAILED "$(payment_sql "SELECT status FROM payments WHERE order_id='$order_id'")" "payment declined once"
  reservation="$(inventory_sql "SELECT status FROM reservations WHERE order_id='$order_id'")"
  case "$reservation" in
    RELEASED)
      assert_equals 1 "$(inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='stock.reserved'")" "reservation event cardinality"
      wait_until 60 "stock.released drained" value_is inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='stock.released' AND status='SENT'" 1 ;;
    SKIPPED)
      assert_equals 0 "$(inventory_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type IN ('stock.reserved','stock.released')")" "skipped reservation emits nothing" ;;
    *) log "unexpected reservation status: $reservation"; return 1 ;;
  esac
  assert_equals 10 "$(inventory_sql "SELECT available FROM inventory_items WHERE sku='$sku'")" "stock fully restored"
  assert_equals 1 "$(payment_sql "SELECT count(*) FROM outbox WHERE correlation_id='$order_id' AND event_type='payment.failed'")" "payment.failed cardinality"
  assert_equals 0 "$(fulfillment_sql "SELECT count(*) FROM fulfillment_state WHERE order_id='$order_id' AND fulfillment_id IS NOT NULL")" "no fulfillment for cancelled order"
  assert_equals 0 "$(gateway_sql "SELECT count(*) FROM dead_letter WHERE event_id IN (SELECT event_id FROM outbox WHERE correlation_id='$order_id')")" "no dead letters"
}

run_verification_scenario() {
  case "$1" in
    invalid-event-not-dispatched) scenario_invalid_event_not_dispatched ;;
    compensation-during-gateway-outage) scenario_compensation_during_gateway_outage ;;
    *) return 2 ;;
  esac
}
