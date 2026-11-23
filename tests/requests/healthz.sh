#!/usr/bin/env bash
# GET /healthz — sanity do gateway-server.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

curl -sS -w "\nHTTP %{http_code}\n" "$GATEWAY_URL/healthz"
