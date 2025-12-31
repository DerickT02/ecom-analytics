import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = "http://localhost:4000";
const TENANT_ID = "tenant_derick";

const MODE: "hot" | "mixed" = "hot"; // "hot" | "mixed"
const UNIQUE_TENANTS = Number(200);

export const options = {
  scenarios: {
    query: {
      executor: "ramping-arrival-rate",
      startRate: Number(__ENV.START_RPS || 10),
      timeUnit: "1s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 20),
      maxVUs: Number(__ENV.MAX_VUS || 200),
      stages: [
        { duration: __ENV.RAMP_UP || "10s", target: Number(__ENV.PEAK_RPS || 50) },
        { duration: __ENV.HOLD || "20s", target: Number(__ENV.PEAK_RPS || 50) },
        { duration: __ENV.RAMP_DOWN || "10s", target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

export default function () {
  let tenant = TENANT_ID;

  if (MODE === "mixed") {
    tenant = `tenant_${(__ITER % UNIQUE_TENANTS) + 1}`;
  }

  // ✅ no day query param anymore
  const url = `${BASE_URL}/daily/${encodeURIComponent(tenant)}`;

  const res = http.get(url, {
    headers: { Accept: "application/json" },
    tags: { endpoint: "daily" },
  });

  check(res, {
    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
    "has body": (r) => (typeof r.body === "string" ? r.body : "").length > 0,
  });

  sleep(Math.random() * 0.1);
}