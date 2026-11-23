import csv
import datetime as dt
import hashlib
import json
import tarfile
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from experiments import analysis
from experiments.protocol import (EVIDENCE_DATABASES, OBSERVATION_FIELDS, batch_file, evidence_name,
                                  expected_batch_keys, load_name)


class StatisticsTest(unittest.TestCase):
    def rewrite_rows(self, run: Path, key: str, mutate) -> None:
        rows = analysis.load_observations(run / "observations.csv")
        target = next(row for row in rows if row["batch_key"] == key)
        mutate(target)
        with (run / "observations.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=OBSERVATION_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        canonical = json.loads(batch_file(run, key).read_text())
        mutate(canonical[0])
        batch_file(run, key).write_text(json.dumps(canonical))

    def make_valid_run(self, raw_root: Path, run_id: str = "run-1", *, canonical_official: bool = False) -> Path:
        run = raw_root / run_id
        run.mkdir(parents=True)
        samples = 100 if canonical_official else 1
        repetitions = 5 if canonical_official else 1
        outages = [30, 60, 120] if canonical_official else [3]
        manifest = {
            "run_id": run_id, "git_commit": "abc", "git_dirty": False, "source_sha256": "digest",
            "experiment": "ALL", "samples": samples, "repetitions": repetitions,
            "warmups": 20 if canonical_official else 1, "outages_seconds": outages,
            "seed": 20260828 if canonical_official else 1,
            "analysis_seed": 20260828 if canonical_official else 1,
            "bootstrap_resamples": 10_000 if canonical_official else 10, "official": True,
        }
        (run / "manifest.json").write_text(json.dumps(manifest))
        keys = expected_batch_keys(manifest)
        (run / "state.json").write_text(json.dumps({"completed": keys}))
        all_rows = []
        for key in keys:
            parts = key.split(":")
            repetition = parts[2] if parts[0] in {"A", "B", "D"} else parts[3]
            batch_rows = []
            for index in range(samples):
                sample_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{key}:{index}"))
                order_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{key}:{index}"))
                row = {field: "" for field in OBSERVATION_FIELDS}
                scenario = "loss" if parts[0] == "A" else "overhead"
                variant = parts[1] if parts[0] in {"A", "B"} else "outbox"
                status = "202"
                terminal = ""
                lost = inconsistent = "false"
                complete = "true"
                if parts[0] == "A" and parts[1] == "direct":
                    status, lost, inconsistent, complete = "503", "true", "true", "false"
                elif parts[0] == "A":
                    terminal = "FULFILLED"
                elif parts[0] == "B":
                    order_id = ""
                elif parts[0] == "C":
                    scenario, terminal = f"{parts[1]}-{parts[2]}", "FULFILLED"
                else:
                    scenario = parts[1]
                    terminal = "FULFILLED" if parts[1] == "happy" else "CANCELLED"
                row.update({
                    "batch_key": key, "experiment": parts[0], "scenario": scenario, "variant": variant,
                    "repetition": repetition, "sample_id": sample_id, "order_id": order_id,
                    "event_id": sample_id if parts[0] == "B" else "", "latency_ms": "1",
                    "http_status": status, "terminal_state": terminal, "lost": lost,
                    "inconsistent": inconsistent, "complete": complete,
                })
                if parts[0] == "C":
                    start = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
                    outage = int(parts[2])
                    confirmed = start + dt.timedelta(milliseconds=100)
                    restore = start + dt.timedelta(seconds=outage)
                    readiness = restore + dt.timedelta(milliseconds=100)
                    terminal_at = restore + dt.timedelta(milliseconds=200)
                    row.update({"outage_started_at": start.isoformat(), "outage_confirmed_at": confirmed.isoformat(),
                                "restore_requested_at": restore.isoformat(), "readiness_recovered_at": readiness.isoformat(),
                                "last_terminal_at": terminal_at.isoformat(), "mttr_seconds": "0.1",
                                "total_recovery_seconds": "0.2"})
                batch_rows.append(row)
                all_rows.append(row)
            path = batch_file(run, key)
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(batch_rows))
            evidence = run / "evidence"
            evidence.mkdir(exist_ok=True)
            name = evidence_name(key)
            correlations = sorted(row["order_id"] or row["sample_id"] for row in batch_rows)
            for database in EVIDENCE_DATABASES:
                (evidence / f"{name}-{database}.json").write_text(json.dumps({"correlation_ids": correlations, "data": {}}))
            (evidence / f"{name}-prometheus.json").write_text(json.dumps({
                "status": "success", "data": {"resultType": "matrix", "result": []},
            }))
            (evidence / f"{name}-logs.txt").write_text("ok")
            load = load_name(key)
            points = []
            for index, row in enumerate(batch_rows):
                tags = {"sample_id": row["sample_id"], "sample_index": str(index), "status": row["http_status"]}
                points.extend([
                    {"type": "Point", "metric": "experiment_latency_ms", "data": {"value": 1, "tags": tags}},
                    {"type": "Point", "metric": "http_req_failed", "data": {"value": 0, "tags": tags}},
                ])
            (run / f"{load}.json").write_text("\n".join(json.dumps(point) for point in points))
            (run / f"{load}-k6-summary.json").write_text(json.dumps({"metrics": {
                "iterations": {"values": {"count": samples}},
                "dropped_iterations": {"values": {"count": 0}},
            }}))
            (run / f"{load}-k6.log").write_text("ok")
        with (run / "observations.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=OBSERVATION_FIELDS)
            writer.writeheader()
            writer.writerows(all_rows)
        return run

    def test_percentile_bootstrap_and_wilson_are_deterministic(self):
        self.assertEqual(analysis.percentile([1, 2, 3, 4], 0.5), 2.5)
        self.assertEqual(
            analysis.bootstrap_mean_ci([1, 2, 3], resamples=50, seed=7),
            analysis.bootstrap_mean_ci([1, 2, 3], resamples=50, seed=7),
        )
        low, high = analysis.wilson_interval(0, 100)
        self.assertEqual(low, 0)
        self.assertGreater(high, 0)

    def test_selective_promotion_contains_only_curated_files(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            run = self.make_valid_run(raw_root, canonical_official=True)
            (run / "large-k6.json").write_text("raw")
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root), \
                    mock.patch.object(analysis, "current_source_identity", return_value=("abc", "digest", False)):
                destination = analysis.analyze("run-1", promote=True)
            self.assertEqual(
                {path.name for path in destination.iterdir()},
                {"manifest.json", "observations.csv", "summary.json", "summary.csv", "report.md",
                 "raw-checksums.sha256", "raw-evidence.tar.gz"},
            )
            with tarfile.open(destination / "raw-evidence.tar.gz") as archive:
                archived = set(archive.getnames())
            self.assertIn("state.json", archived)
            self.assertIn("batches/A_direct_1.json", archived)
            self.assertIn("large-k6.json", archived)
            checksum_lines = (destination / "raw-checksums.sha256").read_text().splitlines()
            self.assertTrue(checksum_lines)
            for line in checksum_lines:
                expected, relative = line.split("  ", 1)
                raw_file = run / relative
                self.assertTrue(raw_file.is_file(), relative)
                self.assertEqual(hashlib.sha256(raw_file.read_bytes()).hexdigest(), expected)
                self.assertNotIn(".analysis.", relative)

    def test_promotion_rejects_source_identity_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            self.make_valid_run(raw_root, canonical_official=True)
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root), \
                    mock.patch.object(analysis, "current_source_identity", return_value=("different", "digest", False)):
                with self.assertRaisesRegex(ValueError, "source does not match"):
                    analysis.analyze("run-1", promote=True)

    def test_promotion_rejects_malformed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            run = self.make_valid_run(raw_root)
            (run / "evidence" / "A_direct_1-gateway.json").write_text("{broken")
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root):
                with self.assertRaisesRegex(ValueError, "invalid JSON evidence"):
                    analysis.analyze("run-1", promote=False)

    def test_promotion_rejects_impossible_direct_status(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            run = self.make_valid_run(raw_root)
            self.rewrite_rows(run, "A:direct:1", lambda row: row.update(http_status="202"))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root), \
                    mock.patch.object(analysis, "current_source_identity", return_value=("abc", "digest", False)):
                with self.assertRaisesRegex(ValueError, "http_status"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_mismatched_k6_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            path = run / "B-baseline-r1.json"
            points = [json.loads(line) for line in path.read_text().splitlines()]
            next(point for point in points if point["metric"] == "experiment_latency_ms")["data"]["value"] = 99
            path.write_text("\n".join(json.dumps(point) for point in points))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "does not match k6 metrics"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_invalid_timeline_ordering(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            self.rewrite_rows(run, "C:gateway:3:1", lambda row: row.update(
                restore_requested_at="2025-12-31T23:59:59+00:00"))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "timeline ordering"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_unscoped_per_saga_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            path = run / "evidence" / "D_happy_1-orders.json"
            document = json.loads(path.read_text())
            document["correlation_ids"] = []
            path.write_text(json.dumps(document))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "unscoped SQL evidence"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_rows_outside_evidence_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            path = run / "evidence" / "D_happy_1-orders.json"
            document = json.loads(path.read_text())
            document["data"] = {"orders": [{"order_id": "00000000-0000-4000-8000-000000000099"}]}
            path.write_text(json.dumps(document))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "outside declared correlation scope"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_evidence_rows_without_correlation_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            path = run / "evidence" / "D_happy_1-orders.json"
            document = json.loads(path.read_text())
            document["data"] = {"orders": [{"status": "FULFILLED"}]}
            path.write_text(json.dumps(document))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "no correlation identity"):
                    analysis.analyze("run-1", promote=False)

    def test_analysis_rejects_readiness_after_last_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            run = self.make_valid_run(raw_root)
            self.rewrite_rows(run, "C:gateway:3:1", lambda row: row.update(
                readiness_recovered_at="2026-01-01T00:00:04+00:00", mttr_seconds="0"))
            with mock.patch.object(analysis, "RAW_ROOT", raw_root):
                with self.assertRaisesRegex(ValueError, "timeline ordering"):
                    analysis.analyze("run-1", promote=False)

    def test_promotion_rejects_noncanonical_official_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            self.make_valid_run(raw_root)
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root):
                with self.assertRaisesRegex(ValueError, "official protocol"):
                    analysis.analyze("run-1", promote=True)

    def test_promotion_rejects_partial_run(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_root = Path(directory) / "raw"
            official_root = Path(directory) / "official"
            run = raw_root / "run-1"
            run.mkdir(parents=True)
            (run / "manifest.json").write_text(json.dumps({
                "run_id": "run-1", "git_commit": "abc", "git_dirty": False, "source_sha256": "digest",
                "experiment": "A", "samples": 1, "repetitions": 1, "warmups": 0,
                "outages_seconds": [3], "seed": 1, "analysis_seed": 1,
                "bootstrap_resamples": 10_000, "official": True,
            }))
            (run / "state.json").write_text(json.dumps({"completed": ["A:direct:1"]}))
            with (run / "observations.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["experiment", "scenario", "variant", "repetition",
                                                                  "batch_key", "latency_ms", "lost", "complete"])
                writer.writeheader()
                writer.writerow({"experiment": "A", "scenario": "loss", "variant": "direct", "repetition": 1,
                                 "batch_key": "A:direct:1", "latency_ms": "1", "lost": "true", "complete": "false"})
            with mock.patch.object(analysis, "RAW_ROOT", raw_root), mock.patch.object(analysis, "OFFICIAL_ROOT", official_root):
                with self.assertRaisesRegex(ValueError, "missing completed batches"):
                    analysis.analyze("run-1", promote=False)


if __name__ == "__main__":
    unittest.main()
