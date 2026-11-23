#!/usr/bin/env python3
"""Análise estatística e promoção seletiva dos resultados dos experimentos."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import shutil
import statistics
import sys
import tarfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.protocol import (BOOTSTRAP_RESAMPLES, EVIDENCE_DATABASES, OFFICIAL_PROTOCOL, batch_file,
                                  evidence_name, expected_batch_keys, load_name)

RAW_ROOT = ROOT / "experiments" / "results" / "raw"
OFFICIAL_ROOT = ROOT / "experiments" / "results" / "official"
DEFAULT_BOOTSTRAP_SEED = 20260828


def percentile(values: Iterable[float], probability: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return math.nan
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def bootstrap_mean_ci(values: Iterable[float], *, resamples: int = BOOTSTRAP_RESAMPLES,
                      seed: int = DEFAULT_BOOTSTRAP_SEED) -> tuple[float, float]:
    sample = [float(value) for value in values]
    if not sample:
        return math.nan, math.nan
    rng = random.Random(seed)
    means = [statistics.fmean(rng.choices(sample, k=len(sample))) for _ in range(resamples)]
    return percentile(means, 0.025), percentile(means, 0.975)


def wilson_interval(successes: int, total: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if total == 0:
        return math.nan, math.nan
    proportion = successes / total
    denominator = 1 + z * z / total
    centre = (proportion + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(proportion * (1 - proportion) / total + z * z / (4 * total * total)) / denominator
    low = max(0.0, centre - margin)
    high = min(1.0, centre + margin)
    # Evita resíduos de ponto flutuante perto dos limites 0 e 1.
    if low < 1e-15:
        low = 0.0
    if 1.0 - high < 1e-15:
        high = 1.0
    return low, high


def load_observations(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def require_json(path: Path) -> None:
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid JSON evidence: {path}") from error


def require_ndjson(path: Path) -> None:
    records = 0
    try:
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    json.loads(line)
                    records += 1
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid NDJSON evidence: {path}") from error
    if records == 0:
        raise ValueError(f"empty NDJSON evidence: {path}")


def parse_boolean(value: str, field: str, key: str) -> bool:
    if value.lower() not in {"true", "false"}:
        raise ValueError(f"invalid {field} in {key}: {value!r}")
    return value.lower() == "true"


def parse_timestamp(value: str, field: str, key: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid {field} in {key}: {value!r}") from error
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp {field} has no timezone in {key}")
    return parsed


def validate_row_semantics(key: str, row: dict[str, str]) -> None:
    parts = key.split(":")
    experiment = parts[0]
    expected_scenario = {"A": "loss", "B": "overhead"}.get(experiment, parts[1])
    expected_variant = parts[1] if experiment in {"A", "B"} else "outbox"
    expected_repetition = parts[2] if experiment in {"A", "B", "D"} else parts[3]
    if experiment == "C":
        expected_scenario = f"{parts[1]}-{parts[2]}"
    expected_fields = {
        "experiment": experiment, "scenario": expected_scenario,
        "variant": expected_variant, "repetition": expected_repetition,
    }
    for field, expected in expected_fields.items():
        if row.get(field) != expected:
            raise ValueError(f"invalid {field} in {key}: got {row.get(field)!r}, expected {expected!r}")
    try:
        sample_id = str(uuid.UUID(row.get("sample_id", "")))
    except ValueError as error:
        raise ValueError(f"invalid sample_id in {key}") from error
    if sample_id != row["sample_id"]:
        raise ValueError(f"noncanonical sample_id in {key}")
    try:
        latency = float(row.get("latency_ms", ""))
    except ValueError as error:
        raise ValueError(f"invalid latency_ms in {key}") from error
    if not math.isfinite(latency) or latency < 0:
        raise ValueError(f"invalid latency_ms in {key}: {latency}")
    try:
        status = int(row.get("http_status", ""))
    except ValueError as error:
        raise ValueError(f"invalid http_status in {key}") from error
    allowed_statuses = {200, 202}
    if experiment == "A" and parts[1] == "direct":
        allowed_statuses = {200, 503}
    elif experiment == "B":
        allowed_statuses = {202, 409} if parts[1] == "gateway" else {202}
    if status not in allowed_statuses:
        raise ValueError(f"invalid http_status in {key}: {status}, allowed={sorted(allowed_statuses)}")
    lost = parse_boolean(row.get("lost", ""), "lost", key)
    inconsistent = parse_boolean(row.get("inconsistent", ""), "inconsistent", key)
    complete = parse_boolean(row.get("complete", ""), "complete", key)
    if experiment == "A" and parts[1] == "direct":
        if inconsistent != lost or complete == lost:
            raise ValueError(f"inconsistent outcome flags in {key}")
        expected_terminal = ""
    elif experiment == "B":
        if lost or inconsistent or not complete:
            raise ValueError(f"inconsistent benchmark outcome flags in {key}")
        expected_terminal = ""
    else:
        if lost or inconsistent or not complete:
            raise ValueError(f"inconsistent successful outcome flags in {key}")
        expected_terminal = "CANCELLED" if experiment == "D" and parts[1] != "happy" else "FULFILLED"
    if row.get("terminal_state", "") != expected_terminal:
        raise ValueError(f"invalid terminal_state in {key}: {row.get('terminal_state')!r}")
    if experiment != "B":
        try:
            uuid.UUID(row.get("order_id", ""))
        except ValueError as error:
            raise ValueError(f"invalid order_id in {key}") from error
    if experiment == "B" and row.get("event_id") != sample_id:
        raise ValueError(f"benchmark event_id does not match sample_id in {key}")
    if experiment == "C":
        timestamps = {field: parse_timestamp(row.get(field, ""), field, key) for field in (
            "outage_started_at", "outage_confirmed_at", "restore_requested_at",
            "readiness_recovered_at", "last_terminal_at")}
        if not (timestamps["outage_started_at"] <= timestamps["outage_confirmed_at"]
                <= timestamps["restore_requested_at"]
                and timestamps["restore_requested_at"] <= timestamps["readiness_recovered_at"]
                <= timestamps["last_terminal_at"]):
            raise ValueError(f"invalid recovery timeline ordering in {key}")
        outage = int(parts[2])
        actual_outage = (timestamps["restore_requested_at"] - timestamps["outage_started_at"]).total_seconds()
        if actual_outage + 0.5 < outage:
            raise ValueError(f"outage duration was not sustained in {key}")
        mttr = (timestamps["last_terminal_at"] - timestamps["readiness_recovered_at"]).total_seconds()
        total = (timestamps["last_terminal_at"] - timestamps["restore_requested_at"]).total_seconds()
        if abs(float(row["mttr_seconds"]) - mttr) > 0.01 or abs(float(row["total_recovery_seconds"]) - total) > 0.01:
            raise ValueError(f"recovery durations do not match timeline in {key}")


def load_k6_measurements(path: Path) -> dict[str, tuple[float, int, int]]:
    partial: dict[str, dict[str, float | int]] = {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            point = json.loads(line)
            if point.get("type") != "Point" or point.get("metric") not in {"experiment_latency_ms", "http_req_failed"}:
                continue
            data = point.get("data", {})
            tags = data.get("tags", {})
            sample_id = tags.get("sample_id")
            sample_index = tags.get("sample_index")
            if not sample_id or sample_index is None:
                raise ValueError(f"measured k6 point lacks sample identity: {path}")
            sample = partial.setdefault(sample_id, {"sample_index": int(sample_index)})
            if sample["sample_index"] != int(sample_index):
                raise ValueError(f"conflicting k6 sample_index for {sample_id}")
            if point["metric"] == "experiment_latency_ms":
                if "latency" in sample:
                    raise ValueError(f"duplicate k6 latency for {sample_id}")
                sample["latency"] = float(data["value"])
            elif tags.get("status"):
                if "status" in sample:
                    raise ValueError(f"duplicate k6 status for {sample_id}")
                sample["status"] = int(tags["status"])
    incomplete = [sample_id for sample_id, sample in partial.items() if not {"latency", "status"} <= set(sample)]
    if incomplete:
        raise ValueError(f"incomplete measured k6 samples in {path}: {incomplete[:5]}")
    return {sample_id: (float(sample["latency"]), int(sample["status"]), int(sample["sample_index"]))
            for sample_id, sample in partial.items()}


def validate_evidence_data_scope(value: object, allowed: set[str], path: Path) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in {"order_id", "correlation_id"} and nested is not None and str(nested) not in allowed:
                raise ValueError(f"evidence row outside declared correlation scope in {path.name}: {key}={nested}")
            validate_evidence_data_scope(nested, allowed, path)
    elif isinstance(value, list):
        for nested in value:
            validate_evidence_data_scope(nested, allowed, path)


def evidence_row_correlations(value: object) -> set[str]:
    correlations: set[str] = set()
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in {"order_id", "correlation_id"} and nested is not None:
                correlations.add(str(nested))
            correlations.update(evidence_row_correlations(nested))
    elif isinstance(value, list):
        for nested in value:
            correlations.update(evidence_row_correlations(nested))
    return correlations


def validate_evidence_collections(data: object, allowed: set[str], path: Path) -> None:
    if not isinstance(data, dict):
        raise ValueError(f"SQL evidence data is not an object in {path.name}")
    for collection, rows in data.items():
        if not isinstance(rows, list):
            raise ValueError(f"SQL evidence collection {collection} is not an array in {path.name}")
        for row in rows:
            correlations = evidence_row_correlations(row)
            if not correlations:
                raise ValueError(f"SQL evidence row has no correlation identity in {path.name}:{collection}")
            if not correlations <= allowed:
                raise ValueError(f"evidence row outside declared correlation scope in {path.name}:{collection}")


def summarize(rows: list[dict[str, str]], *, seed: int = DEFAULT_BOOTSTRAP_SEED,
              resamples: int = BOOTSTRAP_RESAMPLES) -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["experiment"], row["scenario"], row["variant"])].append(row)

    summaries: list[dict[str, object]] = []
    for (experiment, scenario, variant), group in sorted(groups.items()):
        latencies = [float(row["latency_ms"]) for row in group if row.get("latency_ms")]
        mttr_values = [float(row["mttr_seconds"]) for row in group if row.get("mttr_seconds")]
        total_recovery_values = [float(row["total_recovery_seconds"]) for row in group if row.get("total_recovery_seconds")]
        losses = sum(row.get("lost") == "true" for row in group)
        complete = sum(row.get("complete") == "true" for row in group)
        loss_low, loss_high = wilson_interval(losses, len(group))
        complete_low, complete_high = wilson_interval(complete, len(group))
        mean_low, mean_high = bootstrap_mean_ci(latencies, seed=seed, resamples=resamples) if latencies else (math.nan, math.nan)
        summaries.append({
            "experiment": experiment,
            "scenario": scenario,
            "variant": variant,
            "n": len(group),
            "latency_n": len(latencies),
            "latency_mean_ms": statistics.fmean(latencies) if latencies else None,
            "latency_stddev_ms": statistics.stdev(latencies) if len(latencies) > 1 else 0 if latencies else None,
            "latency_p50_ms": percentile(latencies, 0.50) if latencies else None,
            "latency_p95_ms": percentile(latencies, 0.95) if latencies else None,
            "latency_p99_ms": percentile(latencies, 0.99) if latencies else None,
            "latency_mean_ci95_low_ms": None if math.isnan(mean_low) else mean_low,
            "latency_mean_ci95_high_ms": None if math.isnan(mean_high) else mean_high,
            "loss_count": losses,
            "loss_rate": losses / len(group),
            "loss_wilson95_low": loss_low,
            "loss_wilson95_high": loss_high,
            "complete_count": complete,
            "complete_rate": complete / len(group),
            "complete_wilson95_low": complete_low,
            "complete_wilson95_high": complete_high,
            "overhead_ms": None,
            "overhead_percent": None,
            "overhead_component": "",
            "mttr_mean_seconds": statistics.fmean(mttr_values) if mttr_values else None,
            "mttr_p95_seconds": percentile(mttr_values, 0.95) if mttr_values else None,
            "total_recovery_mean_seconds": statistics.fmean(total_recovery_values) if total_recovery_values else None,
        })
    b_rows = {row["variant"]: row for row in summaries if row["experiment"] == "B" and row["scenario"] == "overhead"}
    baseline = b_rows.get("baseline", {}).get("latency_mean_ms")
    schema = b_rows.get("schema", {}).get("latency_mean_ms")
    gateway = b_rows.get("gateway", {}).get("latency_mean_ms")
    comparisons = {
        "schema": (baseline, schema, "schema_validation"),
        "gateway": (schema, gateway, "persistence_after_validation"),
    }
    for variant, (reference, measured, component) in comparisons.items():
        if variant in b_rows and reference is not None and measured is not None:
            delta = float(measured) - float(reference)
            b_rows[variant]["overhead_ms"] = delta
            b_rows[variant]["overhead_percent"] = delta / float(reference) * 100 if float(reference) else None
            b_rows[variant]["overhead_component"] = component
    if "gateway" in b_rows and baseline is not None and gateway is not None:
        b_rows["gateway"]["total_overhead_ms"] = float(gateway) - float(baseline)
        b_rows["gateway"]["total_overhead_percent"] = ((float(gateway) - float(baseline)) / float(baseline) * 100
                                                          if float(baseline) else None)
    for row in summaries:
        row.setdefault("total_overhead_ms", None)
        row.setdefault("total_overhead_percent", None)
    return summaries


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError("cannot write an empty summary")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def render_report(run_id: str, manifest: dict[str, object], summaries: list[dict[str, object]]) -> str:
    lines = [
        f"# Relatório experimental {run_id}", "",
        "## Método executado", "",
        f"- Commit: `{manifest.get('git_commit', 'unknown')}`",
        f"- Árvore dirty: `{manifest.get('git_dirty', 'unknown')}`",
        f"- Amostras por repetição: `{manifest.get('samples')}`",
        f"- Repetições: `{manifest.get('repetitions')}`",
        f"- Seed k6/estatística: `{manifest.get('analysis_seed')}`; bootstrap: `{manifest.get('bootstrap_resamples')}` reamostragens.",
        "- Percentis calculados sobre amostras individuais; proporções usam Wilson 95%.", "",
        "## Resultados", "",
        "| Exp. | Cenário | Variante | n | P50 ms | P95 ms | P99 ms | Perda | Completude |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        def number(name: str) -> str:
            value = row[name]
            return "—" if value is None else f"{float(value):.2f}"
        lines.append(
            f"| {row['experiment']} | {row['scenario']} | {row['variant']} | {row['n']} | "
            f"{number('latency_p50_ms')} | {number('latency_p95_ms')} | {number('latency_p99_ms')} | "
            f"{float(row['loss_rate']):.2%} | {float(row['complete_rate']):.2%} |"
        )
    b_rows = {row["variant"]: row for row in summaries if row["experiment"] == "B"}
    lines.extend(["", "## Decomposição do overhead", ""])
    if "schema" in b_rows:
        lines.append(f"- Validação de schema sobre baseline: `{float(b_rows['schema']['overhead_ms'] or 0):.3f} ms` "
                     f"(`{float(b_rows['schema']['overhead_percent'] or 0):.2f}%`).")
    if "gateway" in b_rows:
        lines.append(f"- Persistência após validação: `{float(b_rows['gateway']['overhead_ms'] or 0):.3f} ms` "
                     f"(`{float(b_rows['gateway']['overhead_percent'] or 0):.2f}%`).")
        lines.append(f"- Overhead total sobre baseline: `{float(b_rows['gateway']['total_overhead_ms'] or 0):.3f} ms` "
                     f"(`{float(b_rows['gateway']['total_overhead_percent'] or 0):.2f}%`).")
    c_rows = [row for row in summaries if row["experiment"] == "C"]
    if c_rows:
        lines.extend(["", "## Recuperação", "", "| Cenário | MTTR médio s | MTTR P95 s | Total médio s |", "|---|---:|---:|---:|"])
        for row in c_rows:
            lines.append(f"| {row['scenario']} | {float(row['mttr_mean_seconds'] or 0):.3f} | "
                         f"{float(row['mttr_p95_seconds'] or 0):.3f} | {float(row['total_recovery_mean_seconds'] or 0):.3f} |")
    lines.extend([
        "", "## Verificação dos critérios", "",
    ])
    by_key = {(row["experiment"], row["variant"]): row for row in summaries}
    gateway = by_key.get(("B", "gateway"))
    if gateway:
        gateway_ok = float(gateway["complete_rate"]) == 1 and float(gateway["latency_p95_ms"] or math.inf) < 250 and float(gateway["latency_p99_ms"] or math.inf) < 500
        lines.append(f"- Experimento B (Gateway sem erros, P95 < 250 ms, P99 < 500 ms): **{'PASS' if gateway_ok else 'FAIL'}**.")
    a_control = by_key.get(("A", "direct"))
    a_outbox = by_key.get(("A", "outbox"))
    if a_control and a_outbox:
        a_ok = (float(a_control["loss_rate"]) > 0 and float(a_outbox["loss_rate"]) == 0
                and float(a_outbox["complete_rate"]) == 1)
        lines.append(f"- Experimento A (controle perde e Outbox não perde): **{'PASS' if a_ok else 'FAIL'}**.")
    d_rows = [row for row in summaries if row["experiment"] == "D"]
    if d_rows:
        d_ok = all(float(row["complete_rate"]) == 1 for row in d_rows)
        lines.append(f"- Experimento D (completude integral): **{'PASS' if d_ok else 'FAIL'}**.")
    c_rows = [row for row in summaries if row["experiment"] == "C"]
    if c_rows:
        c_ok = all(float(row["complete_rate"]) == 1 and float(row["loss_rate"]) == 0 for row in c_rows)
        lines.append(f"- Experimento C (convergência e zero perda em todas as janelas): **{'PASS' if c_ok else 'FAIL'}**.")
    lines.extend([
        "", "## Critérios e limitações", "",
        "Os resultados descrevem somente o host e as imagens registrados no manifesto. "
        "A execução é serial e não representa um benchmark universal de capacidade.", "",
    ])
    return "\n".join(lines)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def current_source_identity() -> tuple[str, str, bool]:
    from experiments.runner import run, source_digest
    commit = run(["git", "rev-parse", "HEAD"], timeout=10).stdout.strip()
    dirty = bool(run(["git", "status", "--porcelain", "--untracked-files=all"], timeout=10).stdout.strip())
    return commit, source_digest(), dirty


def raw_files(source: Path) -> list[Path]:
    return sorted(path for path in source.rglob("*") if path.is_file()
                  and "analysis" not in path.parts
                  and "superseded" not in path.parts
                  and not any(part.startswith(".analysis.") and part.endswith(".tmp")
                              for part in path.relative_to(source).parts))


def validate_run(source: Path, manifest: dict[str, object], rows: list[dict[str, str]], *, promote: bool) -> None:
    required_manifest = {
        "run_id", "git_commit", "git_dirty", "source_sha256", "experiment", "samples", "repetitions",
        "warmups", "outages_seconds", "seed", "analysis_seed", "bootstrap_resamples", "official",
    }
    missing_manifest = sorted(required_manifest - set(manifest))
    if missing_manifest:
        raise ValueError(f"manifest is missing required fields: {', '.join(missing_manifest)}")
    if promote:
        differences = [name for name, expected_value in OFFICIAL_PROTOCOL.items()
                       if manifest.get(name) != expected_value]
        if differences:
            details = ", ".join(f"{name}={manifest.get(name)!r} (expected {OFFICIAL_PROTOCOL[name]!r})"
                                for name in differences)
            raise ValueError(f"promotion requires the canonical official protocol: {details}")
    expected = expected_batch_keys(manifest)
    state_path = source / "state.json"
    if not state_path.exists():
        raise ValueError("run has no state.json")
    completed = json.loads(state_path.read_text(encoding="utf-8")).get("completed", [])
    missing_completed = sorted(set(expected) - set(completed))
    unexpected_completed = sorted(set(completed) - set(expected))
    if missing_completed:
        raise ValueError(f"missing completed batches: {', '.join(missing_completed)}")
    if unexpected_completed:
        raise ValueError(f"unexpected completed batches: {', '.join(unexpected_completed)}")
    if len(completed) != len(set(completed)):
        raise ValueError("state.json contains duplicate completed batches")

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = row.get("batch_key", "")
        if not key:
            raise ValueError("observation without batch_key")
        grouped[key].append(row)
        if not row.get("http_status"):
            raise ValueError(f"observation without http_status in {key}")
        validate_row_semantics(key, row)
        if key.startswith("C:") and any(not row.get(field) for field in (
                "outage_started_at", "outage_confirmed_at", "restore_requested_at",
                "readiness_recovered_at", "last_terminal_at")):
            raise ValueError(f"experiment C observation has incomplete recovery timeline in {key}")
    if set(grouped) != set(expected):
        missing = sorted(set(expected) - set(grouped))
        extra = sorted(set(grouped) - set(expected))
        raise ValueError(f"observation matrix mismatch: missing={missing}, unexpected={extra}")
    samples = int(manifest["samples"])
    for key in expected:
        if len(grouped[key]) != samples:
            raise ValueError(f"batch {key} has {len(grouped[key])} observations, expected {samples}")
        canonical_path = batch_file(source, key)
        if not canonical_path.exists():
            raise ValueError(f"batch {key} has no canonical batch file")
        canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
        if len(canonical) != samples or any(row.get("batch_key") != key for row in canonical):
            raise ValueError(f"canonical batch {key} is invalid")
        def comparable(row: dict[str, object]) -> dict[str, str]:
            return {name: str(value).lower() if isinstance(value, bool) else str(value)
                    for name, value in row.items()}
        canonical_rows = sorted((comparable(row) for row in canonical), key=lambda row: row.get("sample_id", ""))
        csv_rows = sorted((comparable(row) for row in grouped[key]), key=lambda row: row.get("sample_id", ""))
        if canonical_rows != csv_rows:
            raise ValueError(f"canonical batch {key} differs from observations.csv")
        sample_ids = [row.get("sample_id", "") for row in canonical_rows]
        if not all(sample_ids) or len(sample_ids) != len(set(sample_ids)):
            raise ValueError(f"batch {key} has missing or duplicate sample IDs")
        name = evidence_name(key)
        evidence = source / "evidence"
        required_evidence = [evidence / f"{name}-{database}.json" for database in EVIDENCE_DATABASES]
        required_evidence.extend([evidence / f"{name}-prometheus.json", evidence / f"{name}-logs.txt"])
        missing_evidence = [str(path.relative_to(source)) for path in required_evidence if not path.exists()]
        if missing_evidence:
            raise ValueError(f"batch {key} has missing evidence: {', '.join(missing_evidence)}")
        if list(evidence.glob(f"{name}-*.error.txt")):
            raise ValueError(f"batch {key} contains failed evidence captures")
        expected_correlations = sorted(row.get("order_id") or row["sample_id"] for row in grouped[key])
        for path in required_evidence[:-2]:
            require_json(path)
            document = json.loads(path.read_text(encoding="utf-8"))
            if sorted(document.get("correlation_ids", [])) != expected_correlations or "data" not in document:
                raise ValueError(f"batch {key} contains unscoped SQL evidence: {path.name}")
            validate_evidence_data_scope(document["data"], set(expected_correlations), path)
            validate_evidence_collections(document["data"], set(expected_correlations), path)
        require_json(required_evidence[-2])
        prometheus = json.loads(required_evidence[-2].read_text(encoding="utf-8"))
        prometheus_data = prometheus.get("data", {})
        if (prometheus.get("status") != "success" or prometheus_data.get("resultType") != "matrix"
                or not isinstance(prometheus_data.get("result"), list)):
            raise ValueError(f"batch {key} contains invalid Prometheus evidence")
        if not required_evidence[-1].read_text(encoding="utf-8").strip():
            raise ValueError(f"batch {key} contains an empty log capture")
        load = load_name(key)
        required_load = [source / f"{load}.json", source / f"{load}-k6-summary.json", source / f"{load}-k6.log"]
        if any(not path.exists() for path in required_load):
            raise ValueError(f"batch {key} has incomplete k6 artifacts")
        require_ndjson(required_load[0])
        require_json(required_load[1])
        if not required_load[2].read_text(encoding="utf-8").strip():
            raise ValueError(f"batch {key} contains an empty k6 log")
        measurements = load_k6_measurements(required_load[0])
        rows_by_sample = {row["sample_id"]: row for row in grouped[key]}
        if set(measurements) != set(rows_by_sample):
            raise ValueError(f"batch {key} k6 sample identities differ from observations")
        if {measurement[2] for measurement in measurements.values()} != set(range(samples)):
            raise ValueError(f"batch {key} k6 sample_index cardinality is invalid")
        for sample_id, (latency, status, _) in measurements.items():
            row = rows_by_sample[sample_id]
            if not math.isclose(float(row["latency_ms"]), latency, rel_tol=0, abs_tol=1e-9) or int(row["http_status"]) != status:
                raise ValueError(f"batch {key} observation does not match k6 metrics for {sample_id}")
        summary = json.loads(required_load[1].read_text(encoding="utf-8"))
        metrics = summary.get("metrics", {})
        iterations = metrics.get("iterations", {}).get("values", {}).get("count")
        dropped = metrics.get("dropped_iterations", {}).get("values", {}).get("count")
        unexpected = metrics.get("experiment_unexpected_responses", {}).get("values", {}).get("count", 0)
        if iterations != samples or dropped != 0 or unexpected != 0:
            raise ValueError(f"batch {key} has invalid k6 summary metrics")

    for key, group in grouped.items():
        if key.startswith("A:direct:") and not any(parse_boolean(row["lost"], "lost", key) for row in group):
            raise ValueError(f"batch {key} did not demonstrate control loss")

    if promote:
        if manifest["experiment"] != "ALL" or not manifest["official"]:
            raise ValueError("promotion requires an official ALL experiment manifest")
        if manifest["git_dirty"]:
            raise ValueError("promotion requires a clean source manifest")
        commit, digest, dirty = current_source_identity()
        if dirty or commit != manifest["git_commit"] or digest != manifest["source_sha256"]:
            raise ValueError("promotion source does not match the clean immutable manifest")


def write_raw_archive(source: Path, destination: Path, files: list[Path]) -> None:
    with tarfile.open(destination, "w:gz", compresslevel=9) as archive:
        for path in files:
            archive.add(path, arcname=str(path.relative_to(source)), recursive=False)


def analyze(run_id: str, *, promote: bool = False) -> Path:
    source = RAW_ROOT / run_id
    observations_path = source / "observations.csv"
    manifest_path = source / "manifest.json"
    if not observations_path.exists() or not manifest_path.exists():
        raise FileNotFoundError(f"run {run_id!r} is incomplete under {source}")
    rows = load_observations(observations_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_run(source, manifest, rows, promote=promote)
    summaries = summarize(rows, seed=int(manifest["analysis_seed"]), resamples=int(manifest["bootstrap_resamples"]))
    destination = OFFICIAL_ROOT / run_id if promote else source / "analysis"
    if promote and destination.exists():
        raise FileExistsError(f"official destination already exists: {destination}")
    staging = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with observations_path.open(encoding="utf-8") as src, (staging / "observations.csv").open("w", encoding="utf-8") as dst:
        dst.write(src.read())
    (staging / "summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(staging / "summary.csv", summaries)
    (staging / "report.md").write_text(render_report(run_id, manifest, summaries), encoding="utf-8")
    inputs = raw_files(source)
    checksums = []
    for raw_file in inputs:
        checksums.append(f"{file_digest(raw_file)}  {raw_file.relative_to(source)}")
    (staging / "raw-checksums.sha256").write_text("\n".join(checksums) + "\n", encoding="utf-8")
    write_raw_archive(source, staging / "raw-evidence.tar.gz", inputs)
    if [f"{file_digest(path)}  {path.relative_to(source)}" for path in inputs] != checksums:
        raise ValueError("raw inputs changed while promotion artifacts were generated")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    os.replace(staging, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    parser.add_argument("--promote", action="store_true")
    args = parser.parse_args()
    print(analyze(args.run_id, promote=args.promote))


if __name__ == "__main__":
    main()
