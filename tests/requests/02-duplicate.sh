#!/usr/bin/env bash
# Idempotência do gateway: o mesmo event_id enviado duas vezes.
# Primeiro POST → 202. Segundo POST com mesmo event_id → 409 com
# body {"event_id","existing_status"}.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

CID="saga-dup-$(date +%s)"
EVENT_ID=$(uuid)
BODY=$(order_created_envelope "$CID" "$EVENT_ID")

section "POST #1 — esperado 202"
curl -sS -X POST "$GATEWAY_URL/events" -H "Content-Type: application/json" \
  --data-binary "$BODY" -w "\nHTTP %{http_code}\n"

section "POST #2 (mesmo event_id) — esperado 409"
curl -sS -X POST "$GATEWAY_URL/events" -H "Content-Type: application/json" \
  --data-binary "$BODY" -w "\nHTTP %{http_code}\n"

section "Consistência no banco (deve haver 1 linha)"
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  -c "SELECT count(*) FROM outbox WHERE event_id='$EVENT_ID'"
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  -c "SELECT count(*) FROM event_log WHERE event_id='$EVENT_ID' AND direction='RECEIVED'"
