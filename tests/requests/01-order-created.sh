#!/usr/bin/env bash
# POST /events com envelope válido de order.created.
# Esperado: 202 Accepted + body {"event_id": "..."}
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

CID="${1:-saga-01-$(date +%s)}"
BODY=$(order_created_envelope "$CID")

echo "$BODY" | jq .

echo
echo "---"
curl -sS -X POST "$GATEWAY_URL/events" \
  -H "Content-Type: application/json" \
  --data-binary "$BODY" -w "\nHTTP %{http_code}\n"
