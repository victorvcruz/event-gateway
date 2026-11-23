#!/usr/bin/env bash
# lib.sh — helpers compartilhados entre os scripts de tests/
# Uso: source "$(dirname "$0")/lib.sh"

set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
METRICS_URL="${METRICS_URL:-http://localhost:9090}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5438}"
DB_NAME="${DB_NAME:-gateway_db}"
DB_USER="${DB_USER:-gateway}"
DB_PASS="${DB_PASS:-secret}"

# Emite UUID v4 em lowercase (macOS e Linux).
uuid() { uuidgen | tr '[:upper:]' '[:lower:]'; }

# Timestamp ISO-8601 com Z.
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# Cabeçalho colorido para separar seções.
section() { printf "\n\033[1;36m== %s ==\033[0m\n" "$*"; }

# Envelope válido de order.created. Recebe: correlation_id [event_id]
order_created_envelope() {
  local correlation_id="${1:-saga-$(date +%s)}"
  local event_id="${2:-$(uuid)}"
  cat <<EOF
{
  "event_id": "$event_id",
  "event_type": "order.created",
  "correlation_id": "$correlation_id",
  "timestamp": "$(now)",
  "payload": {
    "order_id": "$(uuid)",
    "customer_id": "cust-01",
    "amount_cents": 19900,
    "items": [
      { "sku": "SKU-001", "quantity": 2 }
    ]
  }
}
EOF
}

# Consulta status/sent no outbox por event_id.
outbox_status() {
  local id="$1"
  PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -Atc "SELECT status || '|' || (sent_at IS NOT NULL) FROM outbox WHERE event_id='$id'" 2>/dev/null || true
}

# POST /events. Aceita envelope no stdin. Emite: status_code\n<body>
post_event() {
  curl -sS -o /tmp/gateway-body.$$ -w "%{http_code}" \
    -X POST "$GATEWAY_URL/events" \
    -H "Content-Type: application/json" \
    --data-binary @-
  echo
  cat /tmp/gateway-body.$$
  echo
  rm -f /tmp/gateway-body.$$
}

# Espera outbox.event_id atingir status esperado. Args: event_id target timeout_seconds
wait_status() {
  local id="$1" target="$2" timeout="${3:-15}"
  local start elapsed
  start=$(date +%s)
  while true; do
    local raw
    raw=$(outbox_status "$id")
    if [ -n "$raw" ]; then
      local status="${raw%%|*}"
      if [ "$status" = "$target" ]; then
        return 0
      fi
    fi
    elapsed=$(( $(date +%s) - start ))
    if [ $elapsed -ge $timeout ]; then
      echo "TIMEOUT: $id não atingiu status=$target em ${timeout}s (última leitura: $raw)" >&2
      return 1
    fi
    sleep 0.5
  done
}
