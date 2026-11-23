#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
# shellcheck source=scenarios.sh
source "$SCRIPT_DIR/scenarios.sh"
# shellcheck source=crash_scenarios.sh
source "$SCRIPT_DIR/crash_scenarios.sh"
# shellcheck source=verification_scenarios.sh
source "$SCRIPT_DIR/verification_scenarios.sh"

RESILIENCE_SCENARIOS=(gateway-outage lost-ack fulfillment-outage worker-restart gateway-worker-restart kafka-outage schema-registry-outage database-outage)
VERIFICATION_SCENARIOS=(invalid-event-not-dispatched compensation-during-gateway-outage)
CRASH_SCENARIOS=(order-http-after-commit gateway-ingest-after-commit dispatch-after-business-commit worker-after-gateway-ack kafka-producer-after-ack kafka-consumer-before-offset kafka-offset-rewind payment-compensation-crash inventory-compensation-crash same-sku-contention)

run_named_scenario() {
  case "$1" in
    gateway-outage|lost-ack|fulfillment-outage|worker-restart|gateway-worker-restart|kafka-outage|schema-registry-outage|database-outage|dead-letter)
      run_scenario "$1"
      ;;
    invalid-event-not-dispatched|compensation-during-gateway-outage)
      run_verification_scenario "$1"
      ;;
    *) run_crash_scenario "$1" ;;
  esac
}

on_exit() {
  local status="$?"
  trap - EXIT INT TERM
  if (( status != 0 )); then diagnostics; fi
  recover_faults
  if [[ "${CHAOS_KEEP:-0}" == "1" ]]; then
    log "CHAOS_KEEP=1: stack preserved for inspection"
  else
    stack_down || true
  fi
  release_lock
  exit "$status"
}

acquire_lock
trap on_exit EXIT INT TERM
stack_up

if [[ -n "${SCENARIO:-}" ]]; then
  run_named_scenario "$SCENARIO"
else
  case "${CHAOS_SUITE:-all}" in
    resilience) requested=("${RESILIENCE_SCENARIOS[@]}" dead-letter) ;;
    crash) requested=("${CRASH_SCENARIOS[@]}") ;;
    verification) requested=("${VERIFICATION_SCENARIOS[@]}") ;;
    all) requested=("${RESILIENCE_SCENARIOS[@]}" "${CRASH_SCENARIOS[@]}" "${VERIFICATION_SCENARIOS[@]}" dead-letter) ;;
    *) log "unknown CHAOS_SUITE: ${CHAOS_SUITE}"; exit 2 ;;
  esac
  for scenario in "${requested[@]}"; do
    run_named_scenario "$scenario"
    toxiproxy_reset
    failpoint_reset
  done
fi

log "all requested chaos scenarios passed"
