import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Trend } from "k6/metrics";
import { acceptedBenchmarkStatus, deterministicUUID } from "./identity.mjs";

const samples = Number(__ENV.SAMPLES || "100");
const vus = Math.min(Number(__ENV.VUS || "10"), samples);
const rate = Number(__ENV.RATE || "10");
const variant = __ENV.VARIANT;
const runID = __ENV.RUN_ID;
const repetition = __ENV.REPETITION || "1";
const seed = __ENV.SEED || "20260828";

export const options = {
  scenarios: {
    measured: {
      executor: "shared-iterations",
      vus,
      iterations: samples,
      maxDuration: "5m",
    },
  },
  thresholds: {
    dropped_iterations: ["count==0"],
  },
};

const measuredLatency = new Trend("experiment_latency_ms", true);
const unexpectedResponses = new Counter("experiment_unexpected_responses");

function envelope(iteration) {
  const id = deterministicUUID(`${seed}:${runID}:B:${variant}:${repetition}:${iteration}`);
  return {
    event_id: id,
    event_type: "order.cancelled",
    timestamp: new Date().toISOString(),
    correlation_id: id,
    payload: { order_id: id, reason: "customer_request" },
  };
}

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const event = envelope(iteration);
  const response = http.post(__ENV.TARGET_URL, JSON.stringify(event), {
    headers: { "Content-Type": "application/json" },
    tags: { phase: "measured", experiment: "B", scenario: "overhead", variant, repetition, run_id: runID,
      sample_index: String(iteration), sample_id: event.event_id },
    timeout: "10s",
  });
  measuredLatency.add(response.timings.duration, { experiment: "B", scenario: "overhead", variant, repetition,
    sample_index: String(iteration), sample_id: event.event_id });
  if (!check(response, { accepted: (r) => acceptedBenchmarkStatus(variant, r.status) })) {
    unexpectedResponses.add(1, { experiment: "B", variant, repetition });
  }
  if (rate > 0) {
    sleep(vus / rate);
  }
}

export function handleSummary(data) {
  return { [__ENV.SUMMARY_PATH]: JSON.stringify(data, null, 2) };
}
