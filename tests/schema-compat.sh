#!/usr/bin/env bash
set -euo pipefail

registry_url="${SCHEMA_REGISTRY_URL:-http://localhost:8181}"
run_id="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
subjects=()

schema_body() {
  SCHEMA="$1" python3 -c 'import json,os; print(json.dumps({"schemaType":"JSON","schema":os.environ["SCHEMA"]}))'
}

create_subject() {
  local mode="$1" base="$2" subject
  subject="tcc-poc.compat-$mode-$run_id-value"
  subjects+=("$subject")
  curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "{\"compatibility\":\"$mode\"}" "$registry_url/config/$subject" >/dev/null
  curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "$(schema_body "$base")" "$registry_url/subjects/$subject/versions" >/dev/null
  printf '%s' "$subject"
}

compatible() {
  local subject="$1" candidate="$2" scope="${3:-latest}" path
  path="versions/latest"
  if [[ "$scope" == "all" ]]; then
    path="versions"
  fi
  curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "$(schema_body "$candidate")" \
    "$registry_url/compatibility/subjects/$subject/$path?verbose=true" \
    | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["is_compatible"]).lower())'
}

cleanup() {
  for subject in "${subjects[@]-}"; do
    [[ -n "$subject" ]] || continue
    curl -fsS -X DELETE "$registry_url/subjects/$subject?permanent=true" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

curl -fsS "$registry_url/subjects" >/dev/null

base_open='{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":true}'
requires_name='{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"],"additionalProperties":true}'
base_closed='{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}'
adds_name='{"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id"],"additionalProperties":false}'
any_string='{"type":"string"}'
only_a_enum='{"type":"string","enum":["A"]}'

backward_subject="$(create_subject BACKWARD "$base_open")"
[[ "$(compatible "$backward_subject" "$requires_name")" == "false" ]]
printf 'BACKWARD: incompatibilidade detectada ao adicionar campo obrigatório.\n'

forward_subject="$(create_subject FORWARD "$base_closed")"
[[ "$(compatible "$forward_subject" "$adds_name")" == "false" ]]
printf 'FORWARD: incompatibilidade detectada para leitor anterior fechado.\n'

full_subject="$(create_subject FULL "$base_closed")"
[[ "$(compatible "$full_subject" "$adds_name")" == "false" ]]
printf 'FULL: alteração rejeitada por não ser bidirecional.\n'

# Constrói deliberadamente um histórico incompatível sob NONE. A proposta é
# igual à versão mais recente: FULL a aceita contra latest, enquanto
# FULL_TRANSITIVE a rejeita ao também compará-la com a versão 1.
transitive_subject="$(create_subject NONE "$any_string")"
curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d "$(schema_body "$only_a_enum")" "$registry_url/subjects/$transitive_subject/versions" >/dev/null
curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"FULL"}' "$registry_url/config/$transitive_subject" >/dev/null
[[ "$(compatible "$transitive_subject" "$only_a_enum")" == "true" ]]
curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"FULL_TRANSITIVE"}' "$registry_url/config/$transitive_subject" >/dev/null
[[ "$(compatible "$transitive_subject" "$only_a_enum" all)" == "false" ]]
printf 'FULL_TRANSITIVE: histórico completo considerado (diferença para FULL comprovada).\n'
