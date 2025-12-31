import http from "k6/http";
import { check, sleep } from "k6";

// ---------- CONFIG ----------
const BASE_URL = "http://localhost:3000";
const PATH = "/v1/events";
const TENANT_ID = "tenant_derick";
const DAY = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

const EVENTS_PER_REQ = Number(250); // 100–500 is typical
const UNIQUE_USERS = Number(1000);

// 10k target: (10k / 250) = 40 requests total
// 100k target: (100k / 250) = 400 requests total
const TARGET_EVENTS = Number(100000);

// How many total requests should be sent across the whole test
const TARGET_REQS = Math.ceil(TARGET_EVENTS / EVENTS_PER_REQ);

// ---------- LOAD SHAPE ----------
export const options = {
  // Scenario: ramp traffic up then hold then ramp down.
  scenarios: {
    ingest: {
      executor: "ramping-arrival-rate",
      startRate: Number(__ENV.START_RPS || 2), // requests/sec
      timeUnit: "1s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 20),
      maxVUs: Number(__ENV.MAX_VUS || 200),
      stages: [
        { duration: __ENV.RAMP_UP || "15s", target: Number(__ENV.PEAK_RPS || 10) },
        { duration: __ENV.HOLD || "30s", target: Number(__ENV.PEAK_RPS || 10) },
        { duration: __ENV.RAMP_DOWN || "10s", target: 0 },
      ],
    },
  },

  // Basic guardrails
  thresholds: {
    http_req_failed: ["rate<0.01"],       // <1% errors
    http_req_duration: ["p(95)<1000"],    // 95% under 1s
  },
};

// ---------- EVENT GEN ----------
let globalEventId = 1;

function isoTime(i: number) {
  const mm = String(i % 60).padStart(2, "0");
  const hh = String(Math.floor(i / 60) % 24).padStart(2, "0");
  return `${DAY}T${hh}:${mm}:00.000Z`;
}

function makeEvent(i: number) {
  const user = `anon_${i % UNIQUE_USERS}`;
  const sess = `sess_${Math.floor(i / 10)}`;

  const isOrder = i % 250 === 0; // ~0.4%
  const isCart = i % 25 === 0;   // 4%

  const name = isOrder ? "order.completed" : isCart ? "cart.add" : "page.view";

  const properties = isOrder
    ? {
        orderId: `ord_${i}`,
        orderValue: Number((19.99 + (i % 80)).toFixed(2)),
        currency: "USD",
        itemsCount: 1 + (i % 3),
      }
    : isCart
      ? {
          productId: `sku_${i % 200}`,
          quantity: 1 + (i % 3),
          price: Number((9.99 + (i % 50)).toFixed(2)),
        }
      : {
          path: `/page/${i % 50}`,
          referrer: i % 2 ? "google" : "direct",
        };

  return {
    eventId: 1_000_000 + i,
    name,
    time: isoTime(i),
    source: "web",
    anonymousId: user,
    sessionId: sess,
    properties,
    schemaVersion: 1,
  };
}

// Track how many requests we’ve sent across VUs
// (k6 has no shared atomic counter, so we approximate using iterations;
// if you need exact stop-at-N, I’ll show the Redis/extension approach.)
export default function () {
  const url = `${BASE_URL}${PATH}`;

  // Build a batch
  const start = globalEventId;
  const events = [];
  let printed = 0;
  for (let i = 0; i < EVENTS_PER_REQ; i++) {
    events.push(makeEvent(start + i));
  }
  globalEventId += EVENTS_PER_REQ;

  const payload = JSON.stringify({ tenantId: TENANT_ID, events });
  const res = http.post(url, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { endpoint: "ingest" },
  });
  if ((res.status < 200 || res.status >= 300) && printed < 10) {
  printed++;
  console.error(`FAIL status=${res.status} body=${String(res.body).slice(0, 300)}`);
}

  check(res, {
    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
  });

  // small jitter so VUs aren’t perfectly synchronized
  sleep(Math.random() * 0.25);
}