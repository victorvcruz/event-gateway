"""Matriz canônica dos experimentos e regras de nomes dos artefatos."""

from __future__ import annotations

from pathlib import Path

BOOTSTRAP_RESAMPLES = 10_000
OFFICIAL_PROTOCOL = {
    "experiment": "ALL",
    "samples": 100,
    "repetitions": 5,
    "warmups": 20,
    "outages_seconds": [30, 60, 120],
    "seed": 20260828,
    "analysis_seed": 20260828,
    "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
}

OBSERVATION_FIELDS = [
    "batch_key", "experiment", "scenario", "variant", "repetition", "sample_id", "order_id", "event_id",
    "latency_ms", "http_status", "terminal_state", "lost", "inconsistent", "complete",
    "outage_started_at", "outage_confirmed_at", "restore_requested_at", "readiness_recovered_at",
    "last_terminal_at", "mttr_seconds", "total_recovery_seconds", "notes",
]

EVIDENCE_DATABASES = ("control", "fulfillment", "gateway", "inventory", "notifications", "orders", "payments")


def expected_batch_keys(manifest: dict[str, object]) -> list[str]:
    repetitions = int(manifest["repetitions"])
    outages = [int(value) for value in manifest["outages_seconds"]]
    selected = str(manifest["experiment"])
    experiments = ("A", "B", "C", "D") if selected == "ALL" else (selected,)
    keys: list[str] = []
    if "A" in experiments:
        keys.extend(f"A:{variant}:{repetition}" for repetition in range(1, repetitions + 1)
                    for variant in ("direct", "outbox"))
    if "B" in experiments:
        keys.extend(f"B:{variant}:{repetition}" for repetition in range(1, repetitions + 1)
                    for variant in ("baseline", "schema", "gateway"))
    if "C" in experiments:
        keys.extend(f"C:{target}:{outage}:{repetition}" for target in ("gateway", "kafka")
                    for outage in outages for repetition in range(1, repetitions + 1))
    if "D" in experiments:
        keys.extend(f"D:{scenario}:{repetition}" for scenario in ("happy", "payment-failure", "stock-failure")
                    for repetition in range(1, repetitions + 1))
    return keys


def batch_file(run_dir: Path, key: str) -> Path:
    return run_dir / "batches" / (key.replace(":", "_") + ".json")


def load_name(key: str) -> str:
    parts = key.split(":")
    if parts[0] in {"A", "B", "D"}:
        return f"{parts[0]}-{parts[1]}-r{parts[2]}"
    return f"C-{parts[1]}-{parts[2]}-r{parts[3]}"


def evidence_name(key: str) -> str:
    return key.replace(":", "_")


def protocol_values(source: object) -> dict[str, object]:
    getter = source.get if isinstance(source, dict) else lambda name: getattr(source, name)
    return {
        "experiment": str(getter("experiment")),
        "samples": int(getter("samples")),
        "repetitions": int(getter("repetitions")),
        "warmups": int(getter("warmups")),
        "outages_seconds": [int(value) for value in getter("outages_seconds" if isinstance(source, dict) else "outages")],
        "seed": int(getter("seed")),
        "analysis_seed": int(getter("analysis_seed")) if isinstance(source, dict) else int(getter("seed")),
        "bootstrap_resamples": int(getter("bootstrap_resamples")) if isinstance(source, dict) else BOOTSTRAP_RESAMPLES,
    }
