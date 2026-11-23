#!/usr/bin/env python3
"""Runner isolado e retomável dos experimentos A-D do TCC (e da variante suplementar do C)."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.protocol import (BOOTSTRAP_RESAMPLES, OBSERVATION_FIELDS, OFFICIAL_PROTOCOL, batch_file,
                                  evidence_name, load_name, protocol_values)

RAW_ROOT = ROOT / "experiments" / "results" / "raw"
# Política ampliada da execução suplementar do Experimento C: 10 tentativas e
# espera exponencial a partir de 1 s com teto de 30 s (1+2+4+8+16+4x30 = 151 s).
EXTENDED_RETRY = {"EXPERIMENT_RETRY_MAX_ATTEMPTS": "10", "EXPERIMENT_RETRY_MAX_BACKOFF": "30s"}
PROJECT = "event-gateway-experiments"
LOCK = Path(os.environ.get("EXPERIMENT_LOCK", f"/tmp/{PROJECT}.lock"))
COMPOSE_FILES = [ROOT / "infrastructure" / "docker-compose.yml", ROOT / "infrastructure" / "docker-compose.experiments.yml"]
SERVICES = [
    "gateway-db", "orders-db", "payments-db", "inventory-db", "fulfillment-db", "notifications-db", "control-db",
    "kafka", "kafka-init", "schema-registry", "schema-init",
    "gateway-migrations", "order-migrations", "payment-migrations", "inventory-migrations", "fulfillment-migrations", "notification-migrations",
    "gateway-server", "gateway-worker", "order-service", "order-worker", "payment-service", "payment-worker",
    "inventory-service", "inventory-worker", "fulfillment-service", "fulfillment-worker", "notification-service",
    "experiment-control", "prometheus",
]
PORT_ENV = {
    "GATEWAY_PORT": "28080", "GATEWAY_METRICS_PORT": "29090", "GATEWAY_WORKER_METRICS_PORT": "29091",
    "ORDER_PORT": "28081", "PAYMENT_PORT": "28082", "INVENTORY_PORT": "28083", "FULFILLMENT_PORT": "28084",
    "NOTIFICATION_PORT": "28085", "EXPERIMENT_CONTROL_PORT": "28086", "SCHEMA_REGISTRY_PORT": "28181",
    "PROMETHEUS_PORT": "29099", "KAFKA_PORT": "29092", "GATEWAY_DB_PORT": "25438", "CONTROL_DB_PORT": "25439",
    "ORDERS_DB_PORT": "25433", "PAYMENTS_DB_PORT": "25434", "INVENTORY_DB_PORT": "25435",
    "FULFILLMENT_DB_PORT": "25436", "NOTIFICATIONS_DB_PORT": "25437",
}
DATABASES = {
    "gateway": ("gateway-db", "gateway", "gateway_db"),
    "orders": ("orders-db", "orders", "orders_db"),
    "payments": ("payments-db", "payments", "payments_db"),
    "inventory": ("inventory-db", "inventory", "inventory_db"),
    "fulfillment": ("fulfillment-db", "fulfillment", "fulfillment_db"),
    "notifications": ("notifications-db", "notifications", "notifications_db"),
    "control": ("control-db", "control", "control_db"),
}
class RunnerError(RuntimeError):
    pass


@dataclass(frozen=True)
class Measurement:
    sample_id: str
    sample_index: int
    latency_ms: float
    http_status: int


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def run(command: list[str], *, timeout: float = 300, check: bool = True, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    merged.update(PORT_ENV)
    if env:
        merged.update(env)
    result = subprocess.run(command, cwd=ROOT, env=merged, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    if check and result.returncode:
        raise RunnerError(f"command failed ({result.returncode}): {' '.join(command)}\n{result.stdout[-8000:]}")
    return result


def compose(*arguments: str, timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
    command = ["docker", "compose", "--project-name", PROJECT]
    for file in COMPOSE_FILES:
        command.extend(["-f", str(file)])
    command.extend(arguments)
    return run(command, timeout=timeout, check=check)


def compose_retry(*arguments: str, attempts: int = 3, delay: float = 5, timeout: float = 300) -> subprocess.CompletedProcess[str]:
    """Repete um `compose` que depende de um serviço de infraestrutura ainda em eleição (ex.: líder do Schema Registry)."""
    for attempt in range(1, attempts + 1):
        try:
            return compose(*arguments, timeout=timeout)
        except RunnerError:
            if attempt == attempts:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable")


def acquire_lock() -> None:
    try:
        LOCK.mkdir(parents=False)
        (LOCK / "owner.json").write_text(json.dumps({"pid": os.getpid(), "host": socket.gethostname(), "started_at": utc_now()}))
    except FileExistsError as error:
        owner = (LOCK / "owner.json").read_text() if (LOCK / "owner.json").exists() else "unknown"
        raise RunnerError(f"experiment stack is already locked: {owner}") from error


def release_lock() -> None:
    if LOCK.exists():
        shutil.rmtree(LOCK)


def stack_up() -> None:
    (RAW_ROOT).mkdir(parents=True, exist_ok=True)
    compose("--profile", "tools", "pull", "k6", timeout=300)
    compose("up", "-d", "--build", *SERVICES, timeout=1200)
    wait_http("http://localhost:28080/healthz", {200}, 180)
    wait_http("http://localhost:28081/healthz", {200}, 180)
    wait_http("http://localhost:28086/healthz", {200}, 180)


def stack_down() -> None:
    compose("down", "-v", "--remove-orphans", timeout=300, check=False)


def diagnostics(run_id: str | None = None) -> None:
    print(compose("ps", check=False).stdout)
    print(compose("logs", "--tail", "120", *SERVICES, timeout=120, check=False).stdout)
    if run_id:
        prefix = sql_literal(f"exp-{run_id}-%")
        for database, query in {
            "orders": f"SELECT order_id,customer_id,status,cancel_reason FROM orders WHERE customer_id LIKE {prefix} ORDER BY created_at",
            "control": f"SELECT order_id,event_id,customer_id,run_id FROM control_orders WHERE run_id={sql_literal(run_id)} ORDER BY created_at",
            "gateway": f"SELECT correlation_id,event_type,direction,service_id,logged_at FROM event_log WHERE correlation_id IN (SELECT correlation_id FROM event_log WHERE correlation_id LIKE {prefix}) ORDER BY logged_at",
        }.items():
            try:
                print(f"== {database}\n{sql(database, query)}")
            except Exception as error:  # diagnostics must continue
                print(f"== {database}: {error}")


def wait_until(description: str, predicate: Callable[[], bool], timeout: float = 90, interval: float = 1) -> float:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return time.time()
        except Exception as error:
            last_error = error
        time.sleep(interval)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise RunnerError(f"timeout waiting for {description}{suffix}")


def wait_http(url: str, statuses: set[int], timeout: float) -> float:
    def ready() -> bool:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status in statuses
        except urllib.error.HTTPError as error:
            return error.code in statuses
        except (urllib.error.URLError, TimeoutError):
            return False
    return wait_until(f"HTTP {url} in {sorted(statuses)}", ready, timeout)


def wait_http_unavailable(url: str, timeout: float) -> float:
    def unavailable() -> bool:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status >= 500
        except urllib.error.HTTPError as error:
            return error.code >= 500
        except (urllib.error.URLError, TimeoutError):
            return True
    return wait_until(f"HTTP {url} unavailable", unavailable, timeout)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql(database: str, query: str, *, timeout: float = 10) -> str:
    service, user, dbname = DATABASES[database]
    result = compose("exec", "-T", service, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", dbname, "-c", query,
                     timeout=timeout)
    return result.stdout.strip()


def scalar(database: str, query: str) -> int:
    value = sql(database, query)
    return int(value.splitlines()[-1] if value else "0")


def source_digest() -> str:
    digest = hashlib.sha256()
    files = run(["git", "ls-files", "-co", "--exclude-standard"], timeout=20).stdout.splitlines()
    for relative in sorted(path for path in files if not path.startswith("experiments/results/")):
        path = ROOT / relative
        if path.is_file():
            digest.update(relative.encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


def git_manifest(run_id: str, args: argparse.Namespace) -> dict[str, object]:
    commit = run(["git", "rev-parse", "HEAD"], timeout=10).stdout.strip()
    status = run(["git", "status", "--porcelain", "--untracked-files=all"], timeout=10).stdout
    image_lines = compose("images", "--format", "json", check=False).stdout.splitlines()
    images = []
    for line in image_lines:
        try:
            images.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    k6_image = run(["docker", "image", "inspect", "grafana/k6:2.1.0", "--format", "{{.Id}}"], timeout=20, check=False).stdout.strip()
    return {
        "run_id": run_id, "created_at": utc_now(), "git_commit": commit, "git_dirty": bool(status.strip()),
        "source_sha256": source_digest(), "host": {"hostname": socket.gethostname(), "platform": platform.platform(), "python": platform.python_version()},
        **protocol_values(args), "images": images, "k6_image": {"reference": "grafana/k6:2.1.0", "id": k6_image},
        "ports": PORT_ENV, "official": args.official,
        "retry_policy": getattr(args, "retry_policy", "default"),
        **({"retry_policy_parameters": dict(EXTENDED_RETRY)} if getattr(args, "retry_policy", "default") == "extended" else {}),
    }


def atomic_write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(contents, encoding="utf-8")
    os.replace(temporary, path)


def normalize_row(row: dict[str, object], batch_key: str = "") -> dict[str, object]:
    complete = {field: row.get(field, "") for field in OBSERVATION_FIELDS}
    if batch_key:
        complete["batch_key"] = batch_key
    for boolean in ("lost", "inconsistent", "complete"):
        if isinstance(complete[boolean], bool):
            complete[boolean] = str(complete[boolean]).lower()
    return complete


def write_batch(run_dir: Path, key: str, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise RunnerError(f"cannot checkpoint empty batch {key}")
    normalized = [normalize_row(row, key) for row in rows]
    atomic_write(batch_file(run_dir, key), json.dumps(normalized, indent=2, sort_keys=True) + "\n")


def rebuild_observations(run_dir: Path) -> None:
    state_path = run_dir / "state.json"
    completed = json.loads(state_path.read_text()).get("completed", []) if state_path.exists() else []
    rows: list[dict[str, object]] = []
    for key in completed:
        path = batch_file(run_dir, key)
        if not path.exists():
            raise RunnerError(f"completed batch {key} has no canonical batch file")
        batch_rows = json.loads(path.read_text(encoding="utf-8"))
        if not batch_rows or any(row.get("batch_key") != key for row in batch_rows):
            raise RunnerError(f"canonical batch file is invalid for {key}")
        rows.extend(batch_rows)
    path = run_dir / "observations.csv"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=OBSERVATION_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def completed_key(run_dir: Path, key: str) -> bool:
    state_path = run_dir / "state.json"
    if not state_path.exists():
        return False
    return key in json.loads(state_path.read_text()).get("completed", [])


def mark_completed(run_dir: Path, key: str) -> None:
    state_path = run_dir / "state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"completed": []}
    if key not in state["completed"]:
        state["completed"].append(key)
    state["updated_at"] = utc_now()
    atomic_write(state_path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def commit_batch(run_dir: Path, key: str, rows: list[dict[str, object]]) -> None:
    write_batch(run_dir, key, rows)
    mark_completed(run_dir, key)
    rebuild_observations(run_dir)


def ensure_batch_fresh(run_dir: Path, key: str) -> None:
    load = load_name(key)
    candidates = [
        run_dir / f"{load}.json", run_dir / f"{load}-k6-summary.json", run_dir / f"{load}-k6.log",
        batch_file(run_dir, key),
    ]
    evidence = run_dir / "evidence"
    if evidence.exists():
        candidates.extend(evidence.glob(f"{evidence_name(key)}-*"))
    existing = sorted(str(path.relative_to(run_dir)) for path in candidates if path.exists())
    if existing:
        raise RunnerError(
            f"unsafe resume refused for incomplete batch {key}: artifacts already exist ({', '.join(existing)}); "
            "their requests may have committed, so start a new run_id"
        )


def k6(run_dir: Path, script: str, name: str, variables: dict[str, object]) -> dict[str, Measurement]:
    relative = Path(run_dir.name) / f"{name}.json"
    summary = f"/results/{run_dir.name}/{name}-k6-summary.json"
    protected = [run_dir / f"{name}.json", run_dir / f"{name}-k6-summary.json", run_dir / f"{name}-k6.log"]
    existing = [path.name for path in protected if path.exists()]
    if existing:
        raise RunnerError(f"k6 artifacts already exist and will not be overwritten: {', '.join(existing)}")
    arguments = ["run", "--out", f"json=/results/{relative}"]
    env = {**variables, "SUMMARY_PATH": summary}
    for key, value in env.items():
        arguments.extend(["-e", f"{key}={value}"])
    arguments.append(f"/scripts/{script}")
    result = compose("run", "--rm", "k6", *arguments, timeout=900, check=False)
    (run_dir / f"{name}-k6.log").write_text(result.stdout)
    if result.returncode:
        raise RunnerError(f"k6 failed for {name}\n{result.stdout[-8000:]}")
    return parse_k6(run_dir / f"{name}.json")


def require_load(name: str, measurements: dict[str, Measurement], samples: int,
                 allowed_statuses: set[int]) -> None:
    statuses = [measurement.http_status for measurement in measurements.values()]
    indices = {measurement.sample_index for measurement in measurements.values()}
    if (len(measurements) != samples or indices != set(range(samples))
            or any(status not in allowed_statuses for status in statuses)):
        raise RunnerError(
            f"{name}: load cardinality/error violation: measurements={len(measurements)} indices={sorted(indices)} "
            f"allowed={sorted(allowed_statuses)} actual={dict((status, statuses.count(status)) for status in set(statuses))}"
        )


def parse_k6(path: Path) -> dict[str, Measurement]:
    opener = gzip.open if path.suffix == ".gz" else open
    partial: dict[str, dict[str, object]] = {}
    with opener(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            point = json.loads(line)
            if point.get("type") != "Point":
                continue
            data = point.get("data", {})
            tags = data.get("tags", {})
            if tags.get("phase") == "warmup":
                continue
            sample_id = tags.get("sample_id")
            sample_index = tags.get("sample_index")
            if point.get("metric") in {"experiment_latency_ms", "http_req_failed"} and (not sample_id or sample_index is None):
                raise RunnerError(f"k6 point is missing sample identity in {path}: {point.get('metric')}")
            if not sample_id:
                continue
            sample = partial.setdefault(str(sample_id), {"sample_id": str(sample_id), "sample_index": int(sample_index)})
            if sample["sample_index"] != int(sample_index):
                raise RunnerError(f"conflicting sample_index for {sample_id} in {path}")
            if point.get("metric") == "experiment_latency_ms":
                if "latency_ms" in sample:
                    raise RunnerError(f"duplicate latency for {sample_id} in {path}")
                sample["latency_ms"] = float(data["value"])
            elif point.get("metric") == "http_req_failed" and tags.get("status"):
                if "http_status" in sample:
                    raise RunnerError(f"duplicate HTTP status for {sample_id} in {path}")
                sample["http_status"] = int(tags["status"])
    incomplete = sorted(sample_id for sample_id, sample in partial.items()
                        if "latency_ms" not in sample or "http_status" not in sample)
    if incomplete:
        raise RunnerError(f"incomplete k6 measurements in {path}: {', '.join(incomplete[:5])}")
    return {sample_id: Measurement(**sample) for sample_id, sample in partial.items()}


def seed_inventory(sku: str, available: int) -> None:
    sql("inventory", f"INSERT INTO inventory_items(sku,available) VALUES({sql_literal(sku)},{available}) ON CONFLICT(sku) DO UPDATE SET available=EXCLUDED.available")


def order_rows(prefix: str) -> list[tuple[str, str, str]]:
    output = sql("orders", f"SELECT order_id,customer_id,status FROM orders WHERE customer_id LIKE {sql_literal(prefix + '%')} ORDER BY created_at")
    return [tuple(line.split("|", 2)) for line in output.splitlines() if line]


def control_rows(run_id: str, scenario: str, repetition: int) -> list[tuple[str, str, str]]:
    prefix = f"exp-{run_id}-{scenario}-direct-r{repetition}-"
    output = sql("control", f"SELECT order_id,event_id,customer_id FROM control_orders WHERE run_id={sql_literal(run_id)} AND customer_id LIKE {sql_literal(prefix + '%')} ORDER BY created_at")
    return [tuple(line.split("|", 2)) for line in output.splitlines() if line]


def event_exists(order_id: str, event_type: str = "order.created") -> bool:
    return scalar("gateway", f"SELECT count(*) FROM event_log WHERE correlation_id={sql_literal(order_id)} AND event_type={sql_literal(event_type)} AND direction='RECEIVED'") > 0


def uuid_array(values: list[str]) -> str:
    normalized = [str(uuid.UUID(value)) for value in values]
    return "ARRAY[" + ",".join(sql_literal(value) + "::uuid" for value in normalized) + "]"


def received_order_ids(order_ids: list[str], event_type: str = "order.created") -> set[str]:
    if not order_ids:
        return set()
    normalized = [str(uuid.UUID(value)) for value in order_ids]
    text_ids = "ARRAY[" + ",".join(sql_literal(value) for value in normalized) + "]::text[]"
    output = sql("gateway", f"SELECT DISTINCT correlation_id FROM event_log WHERE correlation_id=ANY({text_ids}) AND event_type={sql_literal(event_type)} AND direction='RECEIVED'")
    return set(filter(None, output.splitlines()))


def terminal_count(prefix: str) -> int:
    return scalar("orders", f"SELECT count(*) FROM orders WHERE customer_id LIKE {sql_literal(prefix + '%')} AND status IN ('FULFILLED','CANCELLED')")


def outboxes_drained(order_ids: list[str]) -> bool:
    if not order_ids:
        return False
    text_ids = "ARRAY[" + ",".join(sql_literal(str(uuid.UUID(value))) for value in order_ids) + "]::text[]"
    for database in ("orders", "payments", "inventory", "fulfillment"):
        if scalar(database, f"SELECT count(*) FROM outbox WHERE payload->>'correlation_id'=ANY({text_ids}) AND status<>'SENT'"):
            return False
    if scalar("gateway", f"SELECT count(*) FROM dispatch_outbox WHERE payload->'envelope'->>'correlation_id'=ANY({text_ids}) AND status<>'SENT'"):
        return False
    return True


def sample_id_from_customer(customer_id: str) -> str:
    candidate = customer_id[-36:]
    try:
        return str(uuid.UUID(candidate))
    except ValueError as error:
        raise RunnerError(f"customer_id does not end in a deterministic sample UUID: {customer_id}") from error


def make_observations(experiment: str, scenario: str, variant: str, repetition: int,
                      orders: list[tuple[str, str, str]], measurements: dict[str, Measurement],
                      **shared: object) -> list[dict[str, object]]:
    rows = []
    for order_id, customer_id, terminal in orders:
        sample_id = sample_id_from_customer(customer_id)
        if sample_id not in measurements:
            raise RunnerError(f"database order {order_id} has no k6 measurement for sample {sample_id}")
        measurement = measurements[sample_id]
        rows.append({"experiment": experiment, "scenario": scenario, "variant": variant, "repetition": repetition,
                     "sample_id": sample_id, "order_id": order_id, "latency_ms": measurement.latency_ms,
                     "http_status": measurement.http_status,
                     "terminal_state": terminal, **shared})
    return rows


def safe_name(value: str) -> str:
    return "".join(character if character.isalnum() or character in "-_" else "_" for character in value)


def capture_evidence(run_dir: Path, key: str, correlation_ids: list[str]) -> None:
    if not correlation_ids:
        raise RunnerError(f"evidence capture for {key} requires correlation IDs")
    normalized_ids = sorted({str(uuid.UUID(value)) for value in correlation_ids})
    correlations = "ARRAY[" + ",".join(sql_literal(value) for value in normalized_ids) + "]::text[]"
    evidence = run_dir / "evidence"
    evidence.mkdir(exist_ok=True)
    name = safe_name(key)
    failures: list[str] = []
    try:
        end = time.time()
        query = urllib.parse.urlencode({"query": "up", "start": end - 600, "end": end, "step": 1})
        with urllib.request.urlopen("http://localhost:29099/api/v1/query_range?" + query, timeout=5) as response:
            prometheus = json.loads(response.read())
        prometheus_data = prometheus.get("data", {})
        if (prometheus.get("status") != "success" or prometheus_data.get("resultType") != "matrix"
                or not isinstance(prometheus_data.get("result"), list)):
            raise RunnerError("Prometheus returned an invalid query response")
        atomic_write(evidence / f"{name}-prometheus.json", json.dumps(prometheus, indent=2, sort_keys=True) + "\n")
        (evidence / f"{name}-prometheus.error.txt").unlink(missing_ok=True)
    except Exception as error:
        (evidence / f"{name}-prometheus.json").unlink(missing_ok=True)
        (evidence / f"{name}-prometheus.error.txt").write_text(str(error))
        failures.append(f"prometheus: {error}")
    snapshots = {
        "gateway": f"""SELECT json_build_object(
            'event_log',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM event_log x WHERE correlation_id=ANY({correlations})),'[]'::jsonb),
            'dispatch_outbox',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM dispatch_outbox x WHERE payload->'envelope'->>'correlation_id'=ANY({correlations})),'[]'::jsonb),
            'dead_letter',COALESCE((SELECT jsonb_agg(to_jsonb(d) || jsonb_build_object('correlation_id',e.correlation_id))
                FROM dead_letter d JOIN event_log e ON e.event_id=d.event_id
                WHERE e.correlation_id=ANY({correlations})),'[]'::jsonb))""",
        "orders": f"""SELECT json_build_object(
            'orders',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM orders x WHERE order_id::text=ANY({correlations})),'[]'::jsonb),
            'outbox',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM outbox x WHERE payload->>'correlation_id'=ANY({correlations})),'[]'::jsonb))""",
        "payments": f"""SELECT json_build_object(
            'payments',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM payments x WHERE order_id::text=ANY({correlations})),'[]'::jsonb),
            'outbox',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM outbox x WHERE payload->>'correlation_id'=ANY({correlations})),'[]'::jsonb))""",
        "inventory": f"""SELECT json_build_object(
            'reservations',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM reservations x WHERE order_id::text=ANY({correlations})),'[]'::jsonb),
            'outbox',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM outbox x WHERE payload->>'correlation_id'=ANY({correlations})),'[]'::jsonb))""",
        "fulfillment": f"""SELECT json_build_object(
            'fulfillment_state',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM fulfillment_state x WHERE order_id::text=ANY({correlations})),'[]'::jsonb),
            'outbox',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM outbox x WHERE payload->>'correlation_id'=ANY({correlations})),'[]'::jsonb))""",
        "notifications": f"SELECT json_build_object('notifications',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM notifications x WHERE order_id::text=ANY({correlations})),'[]'::jsonb))",
        "control": f"SELECT json_build_object('control_orders',COALESCE((SELECT jsonb_agg(to_jsonb(x)) FROM control_orders x WHERE order_id::text=ANY({correlations})),'[]'::jsonb))",
    }
    for database, query in snapshots.items():
        try:
            data = json.loads(sql(database, query))
            atomic_write(evidence / f"{name}-{database}.json", json.dumps({
                "correlation_ids": normalized_ids, "data": data,
            }, indent=2, sort_keys=True) + "\n")
            (evidence / f"{name}-{database}.error.txt").unlink(missing_ok=True)
        except Exception as error:
            (evidence / f"{name}-{database}.json").unlink(missing_ok=True)
            (evidence / f"{name}-{database}.error.txt").write_text(str(error))
            failures.append(f"{database}: {error}")
    try:
        result = compose("logs", "--no-color", "--tail", "80", "gateway-server", "gateway-worker", "order-worker",
                         "experiment-control", timeout=60, check=False)
        if result.returncode != 0:
            raise RunnerError(f"docker compose logs failed ({result.returncode}): {result.stdout[-1000:]}")
        logs = result.stdout
        if not logs.strip():
            raise RunnerError("empty container logs")
        atomic_write(evidence / f"{name}-logs.txt", logs)
        (evidence / f"{name}-logs.error.txt").unlink(missing_ok=True)
    except Exception as error:
        (evidence / f"{name}-logs.txt").unlink(missing_ok=True)
        (evidence / f"{name}-logs.error.txt").write_text(str(error))
        failures.append(f"logs: {error}")
    if failures:
        raise RunnerError(f"evidence capture failed for {key}: {'; '.join(failures)}")


def kafka_answers() -> bool:
    """O broker responde a requisições de API (mesma sonda do healthcheck, sem o intervalo dele)."""
    result = compose("exec", "-T", "kafka", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092",
                     timeout=30, check=False)
    return result.returncode == 0


def restore_kafka(*, resume_workers: bool = True, probe_broker: bool = False) -> float:
    """Restaura o Kafka e devolve o instante (epoch) de prontidão do componente.

    Por padrão é o instante em que o healthcheck do Compose marca o Kafka como saudável. Com
    probe_broker, é o primeiro instante em que o broker responde a requisições de API, que
    antecede o healthcheck e é o que importa quando os workers não estão pausados.
    """
    compose("up", "-d", "kafka", timeout=180)
    answered_at = wait_until("Kafka answers API requests", kafka_answers, 180, interval=0.5) if probe_broker else None
    healthy_at = wait_until("Kafka healthy", lambda: "healthy" in compose("ps", "kafka", "--format", "json", check=False).stdout, 180)
    services = ["kafka-init", "schema-registry", "schema-init"]
    if resume_workers:
        services.extend(["gateway-worker", "experiment-control"])
    compose_retry("up", "-d", *services, timeout=300)
    wait_http("http://localhost:28086/healthz", {200}, 180)
    return answered_at if answered_at is not None else healthy_at


def kafka_stopped() -> bool:
    return not compose("ps", "--status", "running", "-q", "kafka", check=False).stdout.strip()


def benchmark_warmup(target: str, variant: str, repetition: int, count: int, seed: int) -> None:
    for index in range(count):
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}:B:{variant}:{repetition}:warmup:{index}"))
        envelope = {
            "event_id": event_id, "event_type": "order.cancelled", "timestamp": utc_now(),
            "correlation_id": event_id, "payload": {"order_id": event_id, "reason": "customer_request"},
        }
        request = urllib.request.Request(target, data=json.dumps(envelope).encode(), method="POST",
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status != 202:
                    raise RunnerError(f"benchmark warmup {variant} returned {response.status}")
        except urllib.error.HTTPError as error:
            if variant != "gateway" or error.code != 409:
                raise RunnerError(f"benchmark warmup {variant} returned {error.code}") from error
    if variant == "gateway":
        wait_until("Gateway warmup dispatches drained",
                   lambda: scalar("gateway", "SELECT count(*) FROM dispatch_outbox WHERE status<>'SENT'") == 0, 90)


def experiment_a(args: argparse.Namespace, run_dir: Path) -> None:
    sku = f"EXP-A-{args.run_id}"
    seed_inventory(sku, args.samples * args.repetitions * 3)
    for repetition in range(1, args.repetitions + 1):
        for variant in ("direct", "outbox"):
            key = f"A:{variant}:{repetition}"
            if completed_key(run_dir, key):
                continue
            ensure_batch_fresh(run_dir, key)
            wait_http("http://localhost:28086/healthz", {200}, 60)  # warm Registry cache
            compose("kill", "-s", "SIGKILL", "kafka", timeout=30)
            wait_until("Kafka stopped for experiment A", kafka_stopped, 15)
            target = "http://experiment-control:8086/orders/direct" if variant == "direct" else "http://order-service:8081/orders"
            expected = 503 if variant == "direct" else 202
            try:
                measurements = k6(run_dir, "orders.js", f"A-{variant}-r{repetition}", {
                    "EXPERIMENT": "A", "SCENARIO": "loss", "VARIANT": variant, "REPETITION": repetition,
                    "RUN_ID": args.run_id, "SAMPLES": args.samples, "TARGET_URL": target,
                    "EXPECTED_STATUSES": f"{expected},200", "SKU": sku, "SEED": args.seed,
                })
                require_load("experiment A", measurements, args.samples, {expected, 200})
            finally:
                restore_kafka()
            if variant == "outbox":
                prefix = f"exp-{args.run_id}-loss-outbox-r{repetition}-"
                wait_until("Outbox orders to converge", lambda: terminal_count(prefix) == args.samples, 180)
                orders = order_rows(prefix)
                order_ids = [order_id for order_id, _, _ in orders]
                wait_until("experiment A Outboxes drained", lambda: outboxes_drained(order_ids), 120)
                classifications = classify_d_batch(orders, "happy")
                received_ids = received_order_ids(order_ids)
                rows = []
                for order_id, customer_id, terminal in orders:
                    complete, notes = classifications[order_id]
                    sample_id = sample_id_from_customer(customer_id)
                    measurement = measurements[sample_id]
                    rows.append({"experiment": "A", "scenario": "loss", "variant": variant, "repetition": repetition,
                                 "sample_id": sample_id, "order_id": order_id,
                                 "latency_ms": measurement.latency_ms, "http_status": measurement.http_status,
                                 "terminal_state": terminal, "lost": order_id not in received_ids,
                                 "inconsistent": not complete, "complete": complete, "notes": notes})
                if not all(row["complete"] and not row["lost"] for row in rows):
                    raise RunnerError("experiment A Outbox oracle found loss or inconsistent saga")
            else:
                controls = control_rows(args.run_id, "loss", repetition)
                received_ids = received_order_ids([order_id for order_id, _, _ in controls])
                rows = []
                for order_id, event_id, customer_id in controls:
                    received = order_id in received_ids
                    sample_id = sample_id_from_customer(customer_id)
                    measurement = measurements[sample_id]
                    rows.append({"experiment": "A", "scenario": "loss", "variant": variant, "repetition": repetition,
                                 "sample_id": sample_id, "order_id": order_id, "event_id": event_id,
                                 "latency_ms": measurement.latency_ms, "http_status": measurement.http_status,
                                 "terminal_state": "", "lost": not received, "inconsistent": not received, "complete": received})
                if not any(row["lost"] for row in rows):
                    raise RunnerError("experiment A injection invalid: direct dual-write demonstrated no loss")
            if len(rows) != args.samples:
                raise RunnerError(f"experiment A cardinality: got {len(rows)}, want {args.samples}")
            capture_evidence(run_dir, key, [str(row["order_id"]) for row in rows])
            commit_batch(run_dir, key, rows)


def experiment_b(args: argparse.Namespace, run_dir: Path) -> None:
    variants = [
        ("baseline", "http://experiment-control:8086/benchmark/baseline", "http://localhost:28086/benchmark/baseline"),
        ("schema", "http://experiment-control:8086/benchmark/schema", "http://localhost:28086/benchmark/schema"),
        ("gateway", "http://gateway-server:8080/events", "http://localhost:28080/events"),
    ]
    for repetition in range(1, args.repetitions + 1):
        ordered = variants if repetition % 2 else list(reversed(variants))
        for variant, target, warmup_target in ordered:
            key = f"B:{variant}:{repetition}"
            if completed_key(run_dir, key):
                continue
            ensure_batch_fresh(run_dir, key)
            benchmark_warmup(warmup_target, variant, repetition, args.warmups, args.seed)
            measurements = k6(run_dir, "benchmark.js", f"B-{variant}-r{repetition}", {
                "VARIANT": variant, "REPETITION": repetition, "RUN_ID": args.run_id, "SAMPLES": args.samples,
                "TARGET_URL": target, "SEED": args.seed,
            })
            allowed_statuses = {202, 409} if variant == "gateway" else {202}
            require_load(f"experiment B {variant}", measurements, args.samples, allowed_statuses)
            rows = [{"experiment": "B", "scenario": "overhead", "variant": variant, "repetition": repetition,
                                   "sample_id": measurement.sample_id, "event_id": measurement.sample_id,
                                   "latency_ms": measurement.latency_ms,
                                   "http_status": measurement.http_status,
                                   "lost": False, "inconsistent": False, "complete": True}
                    for measurement in sorted(measurements.values(), key=lambda value: value.sample_index)]
            capture_evidence(run_dir, key, list(measurements))
            commit_batch(run_dir, key, rows)


def retry_stats(target: str, order_ids: list[str]) -> str:
    """Resume as tentativas de reenvio de order.created na Outbox responsável pela janela."""
    ids = "ARRAY[" + ",".join(sql_literal(str(uuid.UUID(value))) for value in order_ids) + "]::text[]"
    if target == "gateway":
        database, where = "orders", f"payload->>'correlation_id'=ANY({ids})"
    else:
        database, where = "gateway", f"correlation_id=ANY({ids})"
    row = sql(database, "SELECT coalesce(max(retry_count),0), count(*) FILTER (WHERE retry_count>0), count(*) "
                        f"FROM outbox WHERE event_type='order.created' AND {where}")
    retry_max, retried, total = (int(value) for value in row.split("|"))
    return f"retry_count_max={retry_max};rows_retried={retried};rows={total}"


def experiment_c(args: argparse.Namespace, run_dir: Path) -> None:
    sku = f"EXP-C-{args.run_id}"
    seed_inventory(sku, args.samples * args.repetitions * len(args.outages) * 3)
    for target in ("gateway", "kafka"):
        for outage in args.outages:
            for repetition in range(1, args.repetitions + 1):
                key = f"C:{target}:{outage}:{repetition}"
                if completed_key(run_dir, key):
                    continue
                ensure_batch_fresh(run_dir, key)
                service = "gateway-server" if target == "gateway" else "kafka"
                # Preserva as linhas PENDING do outbox em quedas maiores que o
                # orçamento normal de retry (31 s). O experimento mede a
                # recuperação durável; o esgotamento de retries é coberto pela
                # suíte de chaos.
                protected_worker = "order-worker" if target == "gateway" else "gateway-worker"
                extended = getattr(args, "retry_policy", "default") == "extended"
                outage_started = time.time()
                if extended:
                    # Variante suplementar: o worker reenviador continua ativo e a
                    # política de tentativas (EXTENDED_RETRY) cobre a janela.
                    compose("stop", service, timeout=60)
                else:
                    compose("stop", protected_worker, service, timeout=60)
                if target == "gateway":
                    outage_confirmed = wait_http_unavailable("http://localhost:28080/healthz", 15)
                else:
                    outage_confirmed = wait_until("Kafka stopped for experiment C", kafka_stopped, 15)
                prefix = f"exp-{args.run_id}-{target}-{outage}-outbox-r{repetition}-"
                start = time.time()
                measurements = k6(run_dir, "orders.js", f"C-{target}-{outage}-r{repetition}", {
                    "EXPERIMENT": "C", "SCENARIO": f"{target}-{outage}", "VARIANT": "outbox", "REPETITION": repetition,
                    "RUN_ID": args.run_id, "SAMPLES": args.samples, "TARGET_URL": "http://order-service:8081/orders",
                    "EXPECTED_STATUSES": "202,200", "SKU": sku, "SEED": args.seed,
                })
                require_load("experiment C", measurements, args.samples, {200, 202})
                elapsed = time.time() - outage_started
                if elapsed < outage:
                    time.sleep(outage - elapsed)
                restore_requested = time.time()
                if target == "kafka":
                    kafka_ready_at = restore_kafka(resume_workers=False, probe_broker=extended)
                    readiness = wait_http("http://localhost:28080/healthz", {200}, 180)
                    if extended:
                        # Sem a pausa, os eventos fluem assim que o broker volta a responder; a
                        # prontidão do componente interrompido é esse instante, e não o da
                        # verificação posterior de saúde do Gateway.
                        readiness = kafka_ready_at
                    if not extended:
                        compose_retry("up", "-d", "gateway-worker", timeout=180)
                else:
                    compose("up", "-d", "gateway-server", timeout=180)
                    readiness = wait_http("http://localhost:28080/healthz", {200}, 180)
                    if not extended:
                        compose_retry("up", "-d", "gateway-worker", "order-worker", timeout=180)
                wait_until("C sagas terminal", lambda: terminal_count(prefix) == args.samples, 240)
                orders = order_rows(prefix)
                order_ids = [order_id for order_id, _, _ in orders]
                wait_until("C outboxes drained", lambda: outboxes_drained(order_ids), 120)
                terminal_epoch = sql("orders", f"SELECT extract(epoch FROM max(updated_at)) FROM orders WHERE customer_id LIKE {sql_literal(prefix + '%')}")
                last_terminal = float(terminal_epoch)
                if last_terminal < readiness:
                    raise RunnerError(f"experiment C {target}-{outage}: terminal state predates readiness recovery")
                classifications = classify_d_batch(orders, "happy")
                received_ids = received_order_ids(order_ids)
                rows = make_observations(
                    "C", f"{target}-{outage}", "outbox", repetition, orders, measurements,
                    outage_started_at=dt.datetime.fromtimestamp(outage_started, dt.timezone.utc).isoformat(),
                    outage_confirmed_at=dt.datetime.fromtimestamp(outage_confirmed, dt.timezone.utc).isoformat(),
                    restore_requested_at=dt.datetime.fromtimestamp(restore_requested, dt.timezone.utc).isoformat(),
                    readiness_recovered_at=dt.datetime.fromtimestamp(readiness, dt.timezone.utc).isoformat(),
                    last_terminal_at=dt.datetime.fromtimestamp(last_terminal, dt.timezone.utc).isoformat(),
                    mttr_seconds=last_terminal - readiness, total_recovery_seconds=last_terminal - restore_requested,
                    notes=f"load_started={start}" + (f";retry_policy=extended;{retry_stats(target, order_ids)}" if extended else ""))
                for row in rows:
                    complete, notes = classifications[str(row["order_id"])]
                    row["lost"] = str(row["order_id"]) not in received_ids
                    row["inconsistent"] = not complete
                    row["complete"] = complete
                    row["notes"] = f"{row['notes']};{notes}"
                if len(rows) != args.samples or not all(row["complete"] and not row["lost"] for row in rows):
                    raise RunnerError(f"experiment C {target}-{outage}: loss/completeness/cardinality violation")
                capture_evidence(run_dir, key, order_ids)
                commit_batch(run_dir, key, rows)


def classify_d(order_id: str, scenario: str) -> tuple[bool, str]:
    order_status = sql("orders", f"SELECT status FROM orders WHERE order_id={sql_literal(order_id)}")
    payment = sql("payments", f"SELECT COALESCE(max(status),'') FROM payments WHERE order_id={sql_literal(order_id)}")
    reservation = sql("inventory", f"SELECT COALESCE(max(status),'') FROM reservations WHERE order_id={sql_literal(order_id)}")
    fulfillment = scalar("fulfillment", f"SELECT count(*) FROM fulfillment_state WHERE order_id={sql_literal(order_id)} AND fulfillment_id IS NOT NULL")
    dead = scalar("gateway", f"SELECT count(*) FROM dead_letter WHERE event_id IN (SELECT event_id FROM event_log WHERE correlation_id={sql_literal(order_id)})")
    pending = scalar("gateway", f"SELECT count(*) FROM dispatch_outbox WHERE payload->'envelope'->>'correlation_id'={sql_literal(order_id)} AND status<>'SENT'")
    for database in ("orders", "payments", "inventory", "fulfillment"):
        pending += scalar(database, f"SELECT count(*) FROM outbox WHERE payload->>'correlation_id'={sql_literal(order_id)} AND status<>'SENT'")
    received = set(filter(None, sql("gateway", f"SELECT DISTINCT event_type FROM event_log WHERE correlation_id={sql_literal(order_id)} AND direction='RECEIVED'").splitlines()))
    if scenario == "happy":
        ok = order_status == "FULFILLED" and payment == "PROCESSED" and reservation == "ACTIVE" and fulfillment == 1
        ok = ok and {"order.created", "payment.processed", "stock.reserved", "order.fulfilled"} <= received
    elif scenario == "payment-failure":
        ok = order_status == "CANCELLED" and payment == "FAILED" and reservation in {"SKIPPED", "RELEASED"} and fulfillment == 0
        ok = ok and {"order.created", "payment.failed", "order.cancelled"} <= received
        ok = ok and (reservation != "RELEASED" or "stock.released" in received)
    else:
        ok = order_status == "CANCELLED" and reservation == "FAILED" and payment in {"SKIPPED", "REFUNDED"} and fulfillment == 0
        ok = ok and {"order.created", "stock.failed", "order.cancelled"} <= received
        ok = ok and (payment != "REFUNDED" or "payment.refunded" in received)
    ok = ok and dead == 0 and pending == 0
    return ok, f"payment={payment};reservation={reservation};fulfillment={fulfillment};dlq={dead};non_sent_outbox={pending};events={','.join(sorted(received))}"


def query_status_map(database: str, table: str, order_ids: list[str]) -> dict[str, str]:
    output = sql(database, f"SELECT order_id,status FROM {table} WHERE order_id=ANY({uuid_array(order_ids)})")
    return dict(line.split("|", 1) for line in output.splitlines() if line)


def classify_d_batch(orders: list[tuple[str, str, str]], scenario: str) -> dict[str, tuple[bool, str]]:
    order_ids = [order_id for order_id, _, _ in orders]
    if not order_ids:
        return {}
    ids = uuid_array(order_ids)
    payments = query_status_map("payments", "payments", order_ids)
    reservations = query_status_map("inventory", "reservations", order_ids)
    fulfillments = set(filter(None, sql("fulfillment", f"SELECT order_id FROM fulfillment_state WHERE order_id=ANY({ids}) AND fulfillment_id IS NOT NULL").splitlines()))
    event_output = sql("gateway", f"SELECT correlation_id,event_type,count(*) FROM event_log WHERE correlation_id=ANY(ARRAY[{','.join(sql_literal(value) for value in order_ids)}]::text[]) AND direction='RECEIVED' GROUP BY correlation_id,event_type")
    events: dict[str, dict[str, int]] = {order_id: {} for order_id in order_ids}
    for line in event_output.splitlines():
        if line:
            correlation_id, event_type, count = line.split("|", 2)
            events.setdefault(correlation_id, {})[event_type] = int(count)
    dead_ids = set(filter(None, sql("gateway", f"SELECT DISTINCT e.correlation_id FROM event_log e JOIN dead_letter d USING(event_id) WHERE e.correlation_id=ANY(ARRAY[{','.join(sql_literal(value) for value in order_ids)}]::text[])").splitlines()))
    non_sent_outboxes: set[str] = set()
    gateway_pending = sql("gateway", f"SELECT DISTINCT payload->'envelope'->>'correlation_id' FROM dispatch_outbox WHERE status<>'SENT' AND payload->'envelope'->>'correlation_id'=ANY(ARRAY[{','.join(sql_literal(value) for value in order_ids)}]::text[])")
    non_sent_outboxes.update(filter(None, gateway_pending.splitlines()))
    for database in ("orders", "payments", "inventory", "fulfillment"):
        local_pending = sql(database, f"SELECT DISTINCT payload->>'correlation_id' FROM outbox WHERE status<>'SENT' AND payload->>'correlation_id'=ANY(ARRAY[{','.join(sql_literal(value) for value in order_ids)}]::text[])")
        non_sent_outboxes.update(filter(None, local_pending.splitlines()))
    results = {}
    for order_id, _, order_status in orders:
        payment = payments.get(order_id, "")
        reservation = reservations.get(order_id, "")
        fulfillment = order_id in fulfillments
        received = events.get(order_id, {})
        unique_events = all(count == 1 for count in received.values())
        if scenario == "happy":
            ok = order_status == "FULFILLED" and payment == "PROCESSED" and reservation == "ACTIVE" and fulfillment
            ok = ok and {"order.created", "payment.processed", "stock.reserved", "order.fulfilled"} <= set(received)
        elif scenario == "payment-failure":
            ok = order_status == "CANCELLED" and payment == "FAILED" and reservation in {"SKIPPED", "RELEASED"} and not fulfillment
            ok = ok and {"order.created", "payment.failed", "order.cancelled"} <= set(received)
            ok = ok and (reservation != "RELEASED" or received.get("stock.released") == 1)
        else:
            ok = order_status == "CANCELLED" and reservation == "FAILED" and payment in {"SKIPPED", "REFUNDED"} and not fulfillment
            ok = ok and {"order.created", "stock.failed", "order.cancelled"} <= set(received)
            ok = ok and (payment != "REFUNDED" or received.get("payment.refunded") == 1)
        ok = ok and unique_events and order_id not in dead_ids and order_id not in non_sent_outboxes
        notes = (f"payment={payment};reservation={reservation};fulfillment={int(fulfillment)};"
                 f"dlq={int(order_id in dead_ids)};non_sent_outbox={int(order_id in non_sent_outboxes)};events={json.dumps(received, sort_keys=True)}")
        results[order_id] = (ok, notes)
    return results


def experiment_d(args: argparse.Namespace, run_dir: Path) -> None:
    for scenario in ("happy", "payment-failure", "stock-failure"):
        for repetition in range(1, args.repetitions + 1):
            key = f"D:{scenario}:{repetition}"
            if completed_key(run_dir, key):
                continue
            ensure_batch_fresh(run_dir, key)
            sku = f"EXP-D-{args.run_id}-{scenario}-{repetition}"
            seed_inventory(sku, args.samples + 10)
            prefix = f"exp-{args.run_id}-{scenario}-outbox-r{repetition}-"
            measurements = k6(run_dir, "orders.js", f"D-{scenario}-r{repetition}", {
                "EXPERIMENT": "D", "SCENARIO": scenario, "VARIANT": "outbox", "REPETITION": repetition,
                "RUN_ID": args.run_id, "SAMPLES": args.samples, "TARGET_URL": "http://order-service:8081/orders",
                "EXPECTED_STATUSES": "202,200", "SKU": sku, "SEED": args.seed,
            })
            require_load("experiment D", measurements, args.samples, {200, 202})
            wait_until("D sagas terminal", lambda: terminal_count(prefix) == args.samples, 240)
            observations = []
            orders = order_rows(prefix)
            wait_until("D outboxes drained", lambda: outboxes_drained([order_id for order_id, _, _ in orders]), 120)
            classifications = classify_d_batch(orders, scenario)
            received_ids = received_order_ids([order_id for order_id, _, _ in orders])
            for order_id, customer_id, terminal in orders:
                complete, notes = classifications[order_id]
                sample_id = sample_id_from_customer(customer_id)
                measurement = measurements[sample_id]
                observations.append({"experiment": "D", "scenario": scenario, "variant": "outbox", "repetition": repetition,
                                     "sample_id": sample_id, "order_id": order_id, "latency_ms": measurement.latency_ms,
                                     "http_status": measurement.http_status, "terminal_state": terminal,
                                     "lost": order_id not in received_ids, "inconsistent": not complete,
                                     "complete": complete, "notes": notes})
            if len(observations) != args.samples or not all(row["complete"] and not row["lost"] for row in observations):
                raise RunnerError(f"experiment D {scenario}: completeness/cardinality violation")
            capture_evidence(run_dir, key, [order_id for order_id, _, _ in orders])
            commit_batch(run_dir, key, observations)


EXPERIMENTS = {"A": experiment_a, "B": experiment_b, "C": experiment_c, "D": experiment_d}


def execute(args: argparse.Namespace) -> None:
    args.run_id = args.resume or args.run_id or f"run-{dt.datetime.now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run_dir = RAW_ROOT / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        atomic_write(manifest_path, json.dumps(git_manifest(args.run_id, args), indent=2, sort_keys=True) + "\n")
    else:
        manifest = json.loads(manifest_path.read_text())
        current_digest = source_digest()
        if manifest.get("source_sha256") != current_digest:
            raise RunnerError(
                "resume refused: source tree digest differs from immutable manifest "
                f"({manifest.get('source_sha256')} != {current_digest}); start a new run_id"
            )
        expected_protocol = protocol_values(args)
        actual_protocol = protocol_values(manifest)
        differences = [name for name, expected in expected_protocol.items() if actual_protocol.get(name) != expected]
        if manifest.get("retry_policy", "default") != getattr(args, "retry_policy", "default"):
            differences.append("retry_policy")
            actual_protocol["retry_policy"] = manifest.get("retry_policy", "default")
            expected_protocol["retry_policy"] = getattr(args, "retry_policy", "default")
        if differences:
            details = ", ".join(f"{name}: manifest={actual_protocol.get(name)!r}, requested={expected_protocol[name]!r}"
                                for name in differences)
            raise RunnerError(f"resume refused: protocol parameters differ ({details})")
        rebuild_observations(run_dir)
    selected = list(EXPERIMENTS) if args.experiment == "ALL" else [args.experiment]
    for name in selected:
        EXPERIMENTS[name](args, run_dir)
    from experiments.analysis import analyze
    destination = analyze(args.run_id, promote=args.official and args.experiment == "ALL")
    print(f"run_id={args.run_id}\nresults={destination}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    sub = result.add_subparsers(dest="command", required=True)
    sub.add_parser("up")
    sub.add_parser("down")
    diagnostics_parser = sub.add_parser("diagnostics")
    diagnostics_parser.add_argument("run_id", nargs="?")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--experiment", choices=["A", "B", "C", "D", "ALL"], default="ALL")
    run_parser.add_argument("--run-id")
    run_parser.add_argument("--resume")
    run_parser.add_argument("--samples", type=int, default=100)
    run_parser.add_argument("--repetitions", type=int, default=5)
    run_parser.add_argument("--warmups", type=int, default=20)
    run_parser.add_argument("--outages", type=lambda value: [int(item) for item in value.split(",")], default=[30, 60, 120])
    run_parser.add_argument("--seed", type=int, default=20260828)
    run_parser.add_argument("--official", action="store_true")
    run_parser.add_argument("--retry-policy", choices=["default", "extended"], default="default",
                            help="extended: execução suplementar do Experimento C, sem pausar o worker e com tentativas ampliadas")
    return result


def ensure_official_source_clean() -> None:
    status = run(["git", "status", "--porcelain", "--untracked-files=all"], timeout=10).stdout.strip()
    if status:
        raise RunnerError("official collection requires a clean committed source tree")


def ensure_official_protocol(args: argparse.Namespace) -> None:
    if getattr(args, "retry_policy", "default") != "default":
        raise RunnerError("official run requires the canonical protocol: retry_policy must be 'default'")
    actual = protocol_values(args)
    differences = [name for name, expected in OFFICIAL_PROTOCOL.items() if actual.get(name) != expected]
    if differences:
        details = ", ".join(f"{name}={actual.get(name)!r} (expected {OFFICIAL_PROTOCOL[name]!r})"
                            for name in differences)
        raise RunnerError(f"official run requires the canonical protocol: {details}")


def main() -> None:
    args = parser().parse_args()
    if args.command == "diagnostics":
        diagnostics(args.run_id)
        return
    if args.command == "run" and args.official:
        ensure_official_protocol(args)
        ensure_official_source_clean()
    if args.command == "run" and args.retry_policy == "extended":
        if args.experiment != "C":
            raise RunnerError("--retry-policy extended applies only to --experiment C")
        os.environ.update(EXTENDED_RETRY)
    acquire_lock()
    try:
        if args.command == "up":
            try:
                stack_up()
            except Exception:
                stack_down()
                raise
            return
        if args.command == "down":
            stack_down()
            return
        stack_up()
        try:
            execute(args)
        except Exception:
            diagnostics(getattr(args, "run_id", None) or getattr(args, "resume", None))
            raise
        finally:
            if os.environ.get("EXPERIMENT_KEEP") != "1":
                stack_down()
    finally:
        release_lock()


if __name__ == "__main__":
    try:
        main()
    except (RunnerError, subprocess.TimeoutExpired) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
