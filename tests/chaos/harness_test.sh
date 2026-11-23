#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/event-gateway-chaos-harness.XXXXXX")"
FAKE_BIN="$TEST_ROOT/bin"
DOCKER_LOG="$TEST_ROOT/docker.log"

cleanup() {
  case "$TEST_ROOT" in
    "${TMPDIR:-/tmp}"/event-gateway-chaos-harness.*) rm -rf "$TEST_ROOT" ;;
  esac
}
trap cleanup EXIT

mkdir -p "$FAKE_BIN"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'printf "%s\n" "$*" >> "$FAKE_DOCKER_LOG"' \
  'sleep "${FAKE_DOCKER_SLEEP:-0}"' \
  'if [[ -n "${FAKE_DOCKER_FAIL_MATCH:-}" && "$*" == *"$FAKE_DOCKER_FAIL_MATCH"* ]]; then exit 23; fi' \
  > "$FAKE_BIN/docker"
chmod +x "$FAKE_BIN/docker"
export FAKE_DOCKER_LOG="$DOCKER_LOG"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

test_failed_lock_never_touches_stack() {
  local case_tmp="$TEST_ROOT/run-lock"
  mkdir -p "$case_tmp/event-gateway-chaos.lock"
  : > "$DOCKER_LOG"

  if TMPDIR="$case_tmp/" PATH="$FAKE_BIN:$PATH" bash "$SCRIPT_DIR/run.sh" >/dev/null 2>&1; then
    fail "run.sh should reject an already-held lock"
  fi
  [[ -d "$case_tmp/event-gateway-chaos.lock" ]] || fail "failed acquisition removed the active lock"
  [[ ! -s "$DOCKER_LOG" ]] || fail "failed acquisition invoked Docker"
}

test_chaos_down_respects_lock() {
  local case_tmp="$TEST_ROOT/down-lock"
  mkdir -p "$case_tmp/event-gateway-chaos.lock"
  : > "$DOCKER_LOG"

  if TMPDIR="$case_tmp/" PATH="$FAKE_BIN:$PATH" bash "$SCRIPT_DIR/control.sh" down >/dev/null 2>&1; then
    fail "chaos-down should reject an active run"
  fi
  [[ -d "$case_tmp/event-gateway-chaos.lock" ]] || fail "chaos-down removed the active lock"
  [[ ! -s "$DOCKER_LOG" ]] || fail "chaos-down invoked Docker while locked"
}

test_runtime_paths_are_project_scoped() {
  local case_tmp="$TEST_ROOT/project-paths" first second
  mkdir -p "$case_tmp"

  first="$(TMPDIR="$case_tmp/" CHAOS_PROJECT=chaos-alpha \
    bash -c 'source "$1"; printf "%s|%s" "$CHAOS_LOCK" "$CHAOS_STATE_FILE"' _ "$SCRIPT_DIR/lib.sh")"
  second="$(TMPDIR="$case_tmp/" CHAOS_PROJECT=chaos-beta \
    bash -c 'source "$1"; printf "%s|%s" "$CHAOS_LOCK" "$CHAOS_STATE_FILE"' _ "$SCRIPT_DIR/lib.sh")"

  [[ "$first" != "$second" ]] || fail "different Compose projects share lock/state paths"
  [[ "$first" == *'/chaos-alpha.lock|'*'/chaos-alpha.state' ]] || fail "runtime paths do not include CHAOS_PROJECT"
}

test_failed_up_cleans_partial_stack() {
  local case_tmp="$TEST_ROOT/up-cleanup"
  mkdir -p "$case_tmp"
  : > "$DOCKER_LOG"

  if TMPDIR="$case_tmp/" PATH="$FAKE_BIN:$PATH" FAKE_DOCKER_FAIL_MATCH='up -d --no-build' \
    bash "$SCRIPT_DIR/control.sh" up >/dev/null 2>&1; then
    fail "chaos-up should fail when compose up fails"
  fi
  grep -q 'down -v --remove-orphans' "$DOCKER_LOG" || fail "failed chaos-up did not clean the partial stack"
}

test_failed_down_preserves_diagnostic_state() {
  local case_tmp="$TEST_ROOT/down-state"
  local order_id="81082803-0e03-4569-aa1f-53ccf7848480"
  mkdir -p "$case_tmp"
  : > "$DOCKER_LOG"

  set +e
  TMPDIR="$case_tmp/" PATH="$FAKE_BIN:$PATH" FAKE_DOCKER_FAIL_MATCH='down -v --remove-orphans' \
    bash -c 'source "$1"; remember_order "$2"; stack_down' _ "$SCRIPT_DIR/lib.sh" "$order_id" >/dev/null 2>&1
  local status="$?"
  set -e

  (( status != 0 )) || fail "stack_down should propagate compose cleanup failure"
  [[ -f "$case_tmp/event-gateway-chaos.state" ]] || fail "failed cleanup removed diagnostic state"
}

test_order_state_survives_process() {
  local case_tmp="$TEST_ROOT/state"
  local expected="a57df7ce-cf3e-46be-9638-4a04d9cd17cf" actual
  mkdir -p "$case_tmp"

  TMPDIR="$case_tmp/" bash -c 'source "$1"; remember_order "$2"' _ "$SCRIPT_DIR/lib.sh" "$expected"
  actual="$(TMPDIR="$case_tmp/" bash -c 'source "$1"; load_last_order; printf "%s" "$CHAOS_LAST_ORDER"' _ "$SCRIPT_DIR/lib.sh")"
  [[ "$actual" == "$expected" ]] || fail "persisted order state was not loaded in a new process"
}

test_wait_until_has_hard_timeout() {
  local case_tmp="$TEST_ROOT/sql-timeout" started elapsed status
  mkdir -p "$case_tmp"
  : > "$DOCKER_LOG"
  started="$(date +%s)"

  set +e
  TMPDIR="$case_tmp/" PATH="$FAKE_BIN:$PATH" FAKE_DOCKER_SLEEP=10 CHAOS_SQL_TIMEOUT_SECONDS=20 \
    bash -c 'source "$1"; wait_until 1 "blocked SQL" sql orders-db orders orders_db "SELECT 1"' \
    _ "$SCRIPT_DIR/lib.sh" >/dev/null 2>&1
  status="$?"
  set -e

  elapsed="$(( $(date +%s) - started ))"
  (( status != 0 )) || fail "timed-out wait_until probe returned success"
  (( elapsed < 5 )) || fail "wait_until exceeded its hard timeout (${elapsed}s)"
}

test_failed_lock_never_touches_stack
test_chaos_down_respects_lock
test_runtime_paths_are_project_scoped
test_failed_up_cleans_partial_stack
test_failed_down_preserves_diagnostic_state
test_order_state_survives_process
test_wait_until_has_hard_timeout
PYTHONPATH="$SCRIPT_DIR" python3 -m unittest "$SCRIPT_DIR/controller_test.py"
printf 'chaos harness tests passed\n'
