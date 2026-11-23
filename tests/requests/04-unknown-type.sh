#!/usr/bin/env bash
# event_type sem schema registrado.
# Esperado: 422 com "schema not found for event_type: <t>".
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

BODY=$(cat <<EOF
{
  "event_id": "$(uuid)",
  "event_type": "invented.type",
  "correlation_id": "saga-unknown-$(date +%s)",
  "timestamp": "$(now)",
  "payload": { "anything": 1 }
}
EOF
)

echo "$BODY" | jq .
echo
echo "---"
curl -sS -X POST "$GATEWAY_URL/events" \
  -H "Content-Type: application/json" \
  --data-binary "$BODY" -w "\nHTTP %{http_code}\n"
