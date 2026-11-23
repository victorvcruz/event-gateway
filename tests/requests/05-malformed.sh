#!/usr/bin/env bash
# Envelope JSON malformado.
# Esperado: 400 Bad Request com mensagem do decoder no body.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

curl -sS -X POST "$GATEWAY_URL/events" \
  -H "Content-Type: application/json" \
  --data-binary '{ this is not json' \
  -w "\nHTTP %{http_code}\n"
