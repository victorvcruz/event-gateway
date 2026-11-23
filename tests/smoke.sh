#!/usr/bin/env bash
# smoke.sh — happy path: POST válido → 202 → outbox=SENT → mensagem Kafka.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/infrastructure/docker-compose.yml")
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

CID="saga-smoke-$(date +%s)"
EVENT_ID=$(uuid)

section "1. POST /events (envelope válido)"
order_created_envelope "$CID" "$EVENT_ID" | tee /tmp/envelope-$$.json
printf "\n"
RESP=$(order_created_envelope "$CID" "$EVENT_ID" | post_event)
STATUS_LINE=$(echo "$RESP" | head -1)
echo "response: $RESP"
if [ "$STATUS_LINE" != "202" ]; then
  echo "FAIL: esperava 202, obtive $STATUS_LINE" >&2
  exit 1
fi

section "2. Aguardar Relay marcar SENT"
if wait_status "$EVENT_ID" SENT 10; then
  echo "OK: outbox.event_id=$EVENT_ID → SENT"
else
  echo "FAIL: relay não avançou o evento" >&2
  exit 1
fi

section "3. event_log(RECEIVED)"
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
  -c "SELECT event_id, direction, service_id, logged_at FROM event_log WHERE event_id='$EVENT_ID'"

section "4. Kafka — 1 mensagem com key=correlation_id"
"${COMPOSE[@]}" exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic order.created \
  --from-beginning --max-messages 1 --timeout-ms 5000 \
  --property print.key=true --property print.partition=true

section "5. Métricas expostas"
curl -sS "$METRICS_URL/metrics" | grep -E '^gateway_' | head -20

rm -f /tmp/envelope-$$.json
printf "\n\033[1;32m✓ smoke passou\033[0m\n"
