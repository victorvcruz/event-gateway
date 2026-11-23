import csv
import argparse
import json
import tempfile
import time
import unittest
import subprocess
import urllib.error
from pathlib import Path
from unittest import mock

from experiments import runner


class RunnerHarnessTest(unittest.TestCase):
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b'{"status":"success","data":{"resultType":"matrix","result":[]}}'

    def test_project_lock_is_exclusive_and_recoverable(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(runner, "LOCK", Path(directory) / "lock"):
            runner.acquire_lock()
            with self.assertRaises(runner.RunnerError):
                runner.acquire_lock()
            runner.release_lock()
            runner.acquire_lock()
            runner.release_lock()

    def test_wait_until_has_deadline(self):
        started = time.monotonic()
        with self.assertRaises(runner.RunnerError):
            runner.wait_until("never", lambda: False, timeout=0.03, interval=0.005)
        self.assertLess(time.monotonic() - started, 0.2)

    def test_docker_context_excludes_experiment_evidence(self):
        dockerignore = Path(__file__).parents[2] / ".dockerignore"
        self.assertTrue(dockerignore.is_file())
        patterns = {line.strip().rstrip("/") for line in dockerignore.read_text().splitlines()
                    if line.strip() and not line.lstrip().startswith("#")}
        self.assertIn("experiments/results", patterns)
        self.assertIn(".git", patterns)

    def test_resume_refuses_an_incomplete_batch_with_load_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "A-outbox-r1.json").write_text("original measurements")
            with self.assertRaisesRegex(runner.RunnerError, "unsafe resume"):
                runner.ensure_batch_fresh(run_dir, "A:outbox:1")

    def test_k6_never_overwrites_raw_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run-1"
            run_dir.mkdir()
            raw = run_dir / "A-outbox-r1.json"
            raw.write_text("original measurements")
            with self.assertRaisesRegex(runner.RunnerError, "already exist"):
                runner.k6(run_dir, "orders.js", "A-outbox-r1", {})
            self.assertEqual(raw.read_text(), "original measurements")

    def test_k6_sample_identity_is_seeded_and_deterministic(self):
        script = Path(__file__).parents[1] / "k6" / "identity.mjs"
        program = (
            f'import {{ deterministicUUID }} from "{script.as_uri()}"; '
            'console.log(deterministicUUID("seed:sample")); '
            'console.log(deterministicUUID("seed:sample")); '
            'console.log(deterministicUUID("other:sample"));'
        )
        output = subprocess.check_output(["node", "--input-type=module", "--eval", program], text=True).splitlines()
        self.assertEqual(output[0], output[1])
        self.assertNotEqual(output[0], output[2])

    def test_gateway_benchmark_accepts_duplicate_replay(self):
        script = Path(__file__).parents[1] / "k6" / "identity.mjs"
        program = (
            f'import {{ acceptedBenchmarkStatus }} from "{script.as_uri()}"; '
            'console.log(acceptedBenchmarkStatus("gateway", 202)); '
            'console.log(acceptedBenchmarkStatus("gateway", 409)); '
            'console.log(acceptedBenchmarkStatus("schema", 409));'
        )
        output = subprocess.check_output(["node", "--input-type=module", "--eval", program], text=True).splitlines()
        self.assertEqual(output, ["true", "true", "false"])

    def test_k6_measurements_are_keyed_by_sample_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "load.json"
            points = [
                {"type": "Point", "metric": "experiment_latency_ms", "data": {"time": "2", "value": 20,
                    "tags": {"sample_id": "sample-b", "sample_index": "1"}}},
                {"type": "Point", "metric": "experiment_latency_ms", "data": {"time": "1", "value": 10,
                    "tags": {"sample_id": "sample-a", "sample_index": "0"}}},
                {"type": "Point", "metric": "http_req_failed", "data": {"time": "2", "value": 0,
                    "tags": {"sample_id": "sample-b", "sample_index": "1", "status": "201"}}},
                {"type": "Point", "metric": "http_req_failed", "data": {"time": "1", "value": 0,
                    "tags": {"sample_id": "sample-a", "sample_index": "0", "status": "202"}}},
            ]
            path.write_text("\n".join(json.dumps(point) for point in points))
            measurements = runner.parse_k6(path)
        self.assertEqual(measurements["sample-a"].latency_ms, 10)
        self.assertEqual(measurements["sample-a"].http_status, 202)
        self.assertEqual(measurements["sample-b"].sample_index, 1)

    def test_failed_evidence_capture_prevents_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("prometheus down")), \
                mock.patch.object(runner, "sql", side_effect=runner.RunnerError("database down")), \
                mock.patch.object(runner, "compose", return_value=mock.Mock(stdout="logs")):
            with self.assertRaisesRegex(runner.RunnerError, "evidence capture failed"):
                runner.capture_evidence(Path(directory), "A:outbox:1", ["00000000-0000-4000-8000-000000000001"])

    def test_failed_compose_logs_are_not_accepted_as_evidence(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch("urllib.request.urlopen", return_value=self.Response()), \
                mock.patch.object(runner, "sql", return_value="{}"), \
                mock.patch.object(runner, "compose", return_value=mock.Mock(returncode=1, stdout="docker error")):
            with self.assertRaisesRegex(runner.RunnerError, "logs"):
                runner.capture_evidence(Path(directory), "A:outbox:1", ["00000000-0000-4000-8000-000000000001"])

    def test_malformed_prometheus_response_prevents_checkpoint(self):
        response = self.Response()
        response.read = lambda: b"not-json"
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch("urllib.request.urlopen", return_value=response), \
                mock.patch.object(runner, "sql", return_value="{}"), \
                mock.patch.object(runner, "compose", return_value=mock.Mock(returncode=0, stdout="logs")):
            with self.assertRaisesRegex(runner.RunnerError, "prometheus"):
                runner.capture_evidence(Path(directory), "A:outbox:1", ["00000000-0000-4000-8000-000000000001"])

    def test_sql_evidence_is_scoped_to_each_saga(self):
        correlation_id = "00000000-0000-4000-8000-000000000001"
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch("urllib.request.urlopen", return_value=self.Response()), \
                mock.patch.object(runner, "sql", return_value="[]") as sql_mock, \
                mock.patch.object(runner, "compose", return_value=mock.Mock(returncode=0, stdout="logs")):
            runner.capture_evidence(Path(directory), "A:outbox:1", [correlation_id])
        self.assertTrue(sql_mock.call_args_list)
        self.assertTrue(all(correlation_id in call.args[1] for call in sql_mock.call_args_list))

    def test_gateway_warmup_accepts_duplicate_replay(self):
        duplicate = urllib.error.HTTPError("http://gateway/events", 409, "duplicate", None, None)
        with mock.patch("urllib.request.urlopen", side_effect=duplicate), \
                mock.patch.object(runner, "scalar", return_value=0):
            runner.benchmark_warmup("http://gateway/events", "gateway", 1, 1, 7)

    def test_http_unavailability_is_explicitly_confirmed(self):
        with mock.patch("urllib.request.urlopen", side_effect=runner.urllib.error.URLError("down")):
            confirmed = runner.wait_http_unavailable("http://example.invalid/healthz", timeout=0.05)
        self.assertGreater(confirmed, 0)

    def test_resume_state_and_observation_cardinality(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            row = {
                "experiment": "A", "scenario": "loss", "variant": "outbox", "repetition": 1,
                "sample_id": "sample", "lost": False, "inconsistent": False, "complete": True,
            }
            runner.commit_batch(run_dir, "A:outbox:1", [row])
            runner.commit_batch(run_dir, "A:outbox:1", [row])
            self.assertTrue(runner.completed_key(run_dir, "A:outbox:1"))
            state = json.loads((run_dir / "state.json").read_text())
            self.assertEqual(state["completed"], ["A:outbox:1"])
            with (run_dir / "observations.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["complete"], "true")

    def test_resume_refuses_a_different_source_digest(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(runner, "RAW_ROOT", Path(directory)), \
                mock.patch.object(runner, "source_digest", return_value="new"):
            run_dir = Path(directory) / "fixed-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text(json.dumps({"source_sha256": "old"}))
            args = argparse.Namespace(
                resume="fixed-run", run_id=None, experiment="ALL", samples=10, repetitions=1,
                warmups=2, outages=[3], seed=1, official=False,
            )
            with self.assertRaisesRegex(runner.RunnerError, "resume refused"):
                runner.execute(args)

    def test_official_collection_rejects_dirty_tree(self):
        dirty = mock.Mock(stdout=" M experiments/runner.py\n")
        with mock.patch.object(runner, "run", return_value=dirty):
            with self.assertRaisesRegex(runner.RunnerError, "clean committed"):
                runner.ensure_official_source_clean()

    def test_official_collection_rejects_reduced_protocol(self):
        args = argparse.Namespace(experiment="ALL", samples=10, repetitions=1, warmups=2,
                                  outages=[3], seed=20260828)
        with self.assertRaisesRegex(runner.RunnerError, "canonical protocol"):
            runner.ensure_official_protocol(args)

    def test_official_collection_rejects_extended_retry_policy(self):
        args = argparse.Namespace(experiment="ALL", samples=100, repetitions=5, warmups=20,
                                  outages=[30, 60, 120], seed=20260828, retry_policy="extended")
        with self.assertRaisesRegex(runner.RunnerError, "retry_policy"):
            runner.ensure_official_protocol(args)

    def test_extended_retry_policy_covers_the_longest_outage_window(self):
        attempts = int(runner.EXTENDED_RETRY["EXPERIMENT_RETRY_MAX_ATTEMPTS"])
        cap = int(runner.EXTENDED_RETRY["EXPERIMENT_RETRY_MAX_BACKOFF"].rstrip("s"))
        waits = [min(2 ** retry, cap) for retry in range(attempts - 1)]
        self.assertGreater(sum(waits), 120)

    def test_resume_refuses_different_protocol_parameters(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(runner, "RAW_ROOT", Path(directory)), \
                mock.patch.object(runner, "source_digest", return_value="same"):
            run_dir = Path(directory) / "fixed-run"
            run_dir.mkdir()
            (run_dir / "manifest.json").write_text(json.dumps({
                "source_sha256": "same", "experiment": "ALL", "samples": 100,
                "repetitions": 5, "warmups": 20, "outages_seconds": [30, 60, 120],
                "seed": 7, "analysis_seed": 7, "bootstrap_resamples": 10_000,
            }))
            args = argparse.Namespace(
                resume="fixed-run", run_id=None, experiment="ALL", samples=10, repetitions=5,
                warmups=20, outages=[30, 60, 120], seed=7, official=False,
            )
            with self.assertRaisesRegex(runner.RunnerError, "samples"):
                runner.execute(args)

    def test_batch_commit_replaces_partial_rows_without_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            key = "A:outbox:1"
            old = {"experiment": "A", "scenario": "loss", "variant": "outbox", "repetition": 1,
                   "sample_id": "old", "lost": False, "inconsistent": False, "complete": True}
            new = {**old, "sample_id": "new"}
            runner.write_batch(run_dir, key, [old])  # crash before state checkpoint
            runner.commit_batch(run_dir, key, [new])
            runner.commit_batch(run_dir, key, [new])
            with (run_dir / "observations.csv").open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual([row["sample_id"] for row in rows], ["new"])
            self.assertEqual(rows[0]["batch_key"], key)
            self.assertTrue(runner.completed_key(run_dir, key))

    def test_observations_preserve_http_status_and_recovery_timeline(self):
        sample_id = "00000000-0000-4000-8000-000000000002"
        rows = runner.make_observations(
            "C", "gateway-30", "outbox", 1,
            [("00000000-0000-4000-8000-000000000001", f"customer-{sample_id}", "FULFILLED")],
            {sample_id: runner.Measurement(sample_id, 0, 2.5, 202)},
            outage_started_at="start", outage_confirmed_at="confirmed",
            restore_requested_at="restore", readiness_recovered_at="ready", last_terminal_at="terminal",
            lost=False, inconsistent=False, complete=True,
        )
        self.assertEqual(rows[0]["http_status"], 202)
        self.assertEqual(rows[0]["outage_started_at"], "start")
        self.assertEqual(rows[0]["outage_confirmed_at"], "confirmed")
        self.assertEqual(rows[0]["restore_requested_at"], "restore")

    def test_classification_covers_all_three_saga_outcomes(self):
        cases = {
            "happy": {"order": "FULFILLED", "payment": "PROCESSED", "reservation": "ACTIVE", "fulfillment": 1,
                      "events": "order.created\norder.fulfilled\npayment.processed\nstock.reserved"},
            "payment-failure": {"order": "CANCELLED", "payment": "FAILED", "reservation": "RELEASED", "fulfillment": 0,
                                "events": "order.created\npayment.failed\norder.cancelled\nstock.released"},
            "stock-failure": {"order": "CANCELLED", "payment": "REFUNDED", "reservation": "FAILED", "fulfillment": 0,
                              "events": "order.created\nstock.failed\norder.cancelled\npayment.refunded"},
        }
        for scenario, values in cases.items():
            with self.subTest(scenario=scenario):
                def fake_sql(database, query, **_):
                    if database == "orders": return values["order"]
                    if database == "payments": return values["payment"]
                    if database == "inventory": return values["reservation"]
                    if database == "gateway" and "DISTINCT event_type" in query: return values["events"]
                    return ""

                def fake_scalar(database, query):
                    if database == "fulfillment" and "fulfillment_state" in query: return values["fulfillment"]
                    return 0

                with mock.patch.object(runner, "sql", side_effect=fake_sql), mock.patch.object(runner, "scalar", side_effect=fake_scalar):
                    complete, _ = runner.classify_d("00000000-0000-4000-8000-000000000001", scenario)
                self.assertTrue(complete)

    def test_compensation_event_is_required_when_state_was_compensated(self):
        values = {"order": "CANCELLED", "payment": "FAILED", "reservation": "RELEASED", "fulfillment": 0,
                  "events": "order.created\npayment.failed\norder.cancelled"}

        def fake_sql(database, query, **_):
            if database == "orders": return values["order"]
            if database == "payments": return values["payment"]
            if database == "inventory": return values["reservation"]
            if database == "gateway" and "DISTINCT event_type" in query: return values["events"]
            return ""

        def fake_scalar(database, query):
            if database == "fulfillment" and "fulfillment_state" in query: return values["fulfillment"]
            return 0

        with mock.patch.object(runner, "sql", side_effect=fake_sql), mock.patch.object(runner, "scalar", side_effect=fake_scalar):
            complete, _ = runner.classify_d("00000000-0000-4000-8000-000000000001", "payment-failure")
        self.assertFalse(complete)

    def test_non_sent_outbox_rejects_terminal_saga(self):
        values = {"order": "FULFILLED", "payment": "PROCESSED", "reservation": "ACTIVE", "fulfillment": 1,
                  "events": "order.created\npayment.processed\nstock.reserved\norder.fulfilled"}

        def fake_sql(database, query, **_):
            if database == "orders" and query.startswith("SELECT status"): return values["order"]
            if database == "payments" and "max(status)" in query: return values["payment"]
            if database == "inventory" and "max(status)" in query: return values["reservation"]
            if database == "gateway" and "DISTINCT event_type" in query: return values["events"]
            return ""

        def fake_scalar(database, query):
            if database == "fulfillment" and "fulfillment_state" in query: return values["fulfillment"]
            if database == "gateway" and "dispatch_outbox" in query: return 1
            return 0

        with mock.patch.object(runner, "sql", side_effect=fake_sql), mock.patch.object(runner, "scalar", side_effect=fake_scalar):
            complete, _ = runner.classify_d("00000000-0000-4000-8000-000000000001", "happy")
        self.assertFalse(complete)


if __name__ == "__main__":
    unittest.main()
