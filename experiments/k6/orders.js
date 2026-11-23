import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Trend } from "k6/metrics";
import { deterministicUUID } from "./identity.mjs";

const samples = Number(__ENV.SAMPLES || "100");
const vus = Math.min(Number(__ENV.VUS || "10"), samples);
const rate = Number(__ENV.RATE || "10");
const variant = __ENV.VARIANT || "outbox";
const scenario = __ENV.SCENARIO || "happy";
const runID = __ENV.RUN_ID;
const repetition = __ENV.REPETITION || "1";
const expectedStatuses = new Set((__ENV.EXPECTED_STATUSES || __ENV.EXPECTED_STATUS || "202")
  .split(",").map((value) => Number(value.trim())));
const seed = __ENV.SEED || "20260828";

export const options = {
  scenarios: {
    measured: {
      executor: "shared-iterations",
      vus,
      iterations: samples,
      maxDuration: __ENV.MAX_DURATION || "5m",
    },
  },
  thresholds: {
    dropped_iterations: ["count==0"],
  },
};

const measuredLatency = new Trend("experiment_latency_ms", true);
const unexpectedResponses = new Counter("experiment_unexpected_responses");

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const sampleID = deterministicUUID(`${seed}:${runID}:${scenario}:${variant}:${repetition}:${iteration}`);
  const customerID = `exp-${runID}-${scenario}-${variant}-r${repetition}-${sampleID}`;
  const amount = scenario === "payment-failure" ? 100001 : 1000;
  const sku = scenario === "stock-failure" ? `MISSING-${runID}-${repetition}` : __ENV.SKU;
  const body = {
    customer_id: customerID,
    amount_cents: amount,
    items: [{ sku, quantity: 1 }],
  };
  if (variant === "direct") {
    body.run_id = runID;
  }

  const response = http.post(__ENV.TARGET_URL, JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `${runID}-${scenario}-${variant}-${repetition}-${sampleID}`,
    },
    tags: { experiment: __ENV.EXPERIMENT, scenario, variant, repetition, run_id: runID,
      sample_index: String(iteration), sample_id: sampleID },
    timeout: __ENV.REQUEST_TIMEOUT || "10s",
  });
  measuredLatency.add(response.timings.duration, { experiment: __ENV.EXPERIMENT, scenario, variant, repetition,
    sample_index: String(iteration), sample_id: sampleID });
  if (!check(response, { "expected response": (r) => expectedStatuses.has(r.status) })) {
    unexpectedResponses.add(1, { experiment: __ENV.EXPERIMENT, scenario, variant, repetition });
  }
  if (rate > 0) {
    sleep(vus / rate);
  }
}

export function handleSummary(data) {
  return { [__ENV.SUMMARY_PATH]: JSON.stringify(data, null, 2) };
}
