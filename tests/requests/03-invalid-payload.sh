#!/usr/bin/env bash
# Payload que não valida contra o JSON Schema.
# Esperado: 422 Unprocessable Entity com body text/plain.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

CID="saga-invalid-$(date +%s)"

# amount_cents obrigatório e inteiro > 0; passamos string.
BODY=$(cat <<EOF
{
  "event_id": "$(uuid)",
  "event_type": "order.created",
  "correlation_id": "$CID",
  "timestamp": "$(now)",
  "payload": {
    "order_id": "$(uuid)",
    "customer_id": "cust-01",
    "amount_cents": "cento e noventa e nove"
  }
}
EOF
)

echo "$BODY" | jq .
echo
echo "---"
curl -sS -X POST "$GATEWAY_URL/events" \
  -H "Content-Type: application/json" \
  --data-binary "$BODY" -w "\nHTTP %{http_code}\n"
