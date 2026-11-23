#!/usr/bin/env bash
# GET /metrics (Prometheus). Filtra só as métricas gateway_*.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib.sh"

curl -sS "$METRICS_URL/metrics" | grep -E '^(#\s+HELP gateway_|#\s+TYPE gateway_|gateway_)'
