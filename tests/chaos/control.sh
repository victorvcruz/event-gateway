#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

case "${1:-}" in
  up)
    acquire_lock
    trap release_lock EXIT
    if stack_up; then
      :
    else
      status="$?"
      log "stack startup failed; removing partially-created resources"
      stack_down || log "partial stack cleanup failed; diagnostic state was preserved"
      exit "$status"
    fi
    ;;
  down)
    acquire_lock
    trap release_lock EXIT
    recover_faults
    stack_down
    ;;
  diagnostics)
    diagnostics "${2:-}"
    ;;
  *)
    echo "usage: $0 {up|down|diagnostics}" >&2
    exit 2
    ;;
esac
