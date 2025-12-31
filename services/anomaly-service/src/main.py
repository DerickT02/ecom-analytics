import os
import json
from datetime import date
from typing import Any, Dict, List

from dotenv import load_dotenv
import clickhouse_connect
from pydantic import BaseModel, Field

from openai import OpenAI

load_dotenv()

# ----------------------------
# Config
# ----------------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

LOOKBACK_DAYS = int(os.getenv("ANOMALY_LOOKBACK_DAYS", "14"))
BASELINE_DAYS = int(os.getenv("ANOMALY_BASELINE_DAYS", "7"))
SPIKE_RATIO = float(os.getenv("ANOMALY_SPIKE_RATIO", "2.0"))
DROP_RATIO = float(os.getenv("ANOMALY_DROP_RATIO", "0.5"))

ANOMALY_EVENT_NAME = os.getenv("ANOMALY_EVENT_NAME", "order.completed")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
SUMMARY_MAX_BULLETS = int(os.getenv("SUMMARY_MAX_BULLETS", "6"))

# Exclude backend/system events from DAU by default
EXCLUDE_BACKEND_FROM_DAU = os.getenv("EXCLUDE_BACKEND_FROM_DAU", "true").lower() in (
    "1",
    "true",
    "yes",
)


# ----------------------------
# ClickHouse client
# ----------------------------
def ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


# ----------------------------
# Anomaly logic
# ----------------------------
def severity_from_ratio(ratio: float, direction: str) -> str:
    if direction == "spike":
        if ratio >= 5:
            return "high"
        if ratio >= 3:
            return "medium"
        return "low"
    # drop
    if ratio <= 0.2:
        return "high"
    if ratio <= 0.35:
        return "medium"
    return "low"


def detect_daily_count_anomalies(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    rows: sorted by (tenant_id, day) with {tenant_id, day, value}
    baseline: trailing avg of previous BASELINE_DAYS points (excluding current)
    """
    anomalies: List[Dict[str, Any]] = []
    by_tenant: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        by_tenant.setdefault(r["tenant_id"], []).append(r)

    for tenant_id, series in by_tenant.items():
        series.sort(key=lambda x: x["day"])
        values = [int(s["value"]) for s in series]
        days = [s["day"] for s in series]

        for i in range(BASELINE_DAYS, len(series)):
            current = values[i]
            window = values[i - BASELINE_DAYS : i]
            baseline = sum(window) / max(1, len(window))
            if baseline == 0:
                continue

            ratio = current / baseline
            direction = None
            if ratio >= SPIKE_RATIO:
                direction = "spike"
            elif ratio <= DROP_RATIO:
                direction = "drop"

            if not direction:
                continue

            anomalies.append(
                {
                    "tenant_id": tenant_id,
                    "day": days[i],
                    "metric": f"events.{ANOMALY_EVENT_NAME}.daily_count",
                    "value": int(current),
                    "baseline": float(baseline),
                    "ratio": float(ratio),
                    "direction": direction,
                    "severity": severity_from_ratio(float(ratio), direction),
                    "details": {"event_name": ANOMALY_EVENT_NAME},
                }
            )

    return anomalies


def write_anomalies(ch, anomalies: List[Dict[str, Any]]) -> None:
    if not anomalies:
        return

    data = []
    for a in anomalies:
        data.append(
            (
                a["tenant_id"],
                a["metric"],
                a["day"],
                int(a["value"]),
                float(a["baseline"]),
                float(a["ratio"]),
                a["direction"],
                a["severity"],
                json.dumps(a["details"]),
            )
        )

    ch.insert(
        table="anomalies",
        data=data,
        column_names=[
            "tenant_id",
            "metric",
            "day",
            "value",
            "baseline",
            "ratio",
            "direction",
            "severity",
            "details",
        ],
    )


# ----------------------------
# Summary (ChatGPT) schema
# ----------------------------
class SummaryHighlights(BaseModel):
    headline: str = Field(..., description="One-sentence main takeaway for the day")
    bullets: List[str] = Field(..., description="Short bullets describing what happened")
    recommended_actions: List[str] = Field(
        default_factory=list, description="Optional next steps to investigate"
    )


class DailySummary(BaseModel):
    summary_md: str
    highlights: SummaryHighlights


def build_daily_facts(ch, tenant_id: str, day: date) -> Dict[str, Any]:
    # Top events
    q_top = """
    SELECT name, count() AS c
    FROM events_raw
    WHERE tenant_id = %(tenant)s AND toDate(time) = %(day)s
    GROUP BY name
    ORDER BY c DESC
    LIMIT 10
    """
    top = ch.query(q_top, parameters={"tenant": tenant_id, "day": day}).result_rows
    top_events = [{"name": r[0], "count": int(r[1])} for r in top]
    total_events_top10 = sum(e["count"] for e in top_events)

    # DAU (exclude backend by default)
    if EXCLUDE_BACKEND_FROM_DAU:
        q_dau = """
        SELECT uniqExact(anonymous_id)
        FROM events_raw
        WHERE tenant_id = %(tenant)s
          AND toDate(time) = %(day)s
          AND source != 'backend'
        """
    else:
        q_dau = """
        SELECT uniqExact(anonymous_id)
        FROM events_raw
        WHERE tenant_id = %(tenant)s AND toDate(time) = %(day)s
        """
    dau = int(
        ch.query(q_dau, parameters={"tenant": tenant_id, "day": day}).result_rows[0][0]
    )

    # Orders + revenue (FIXED: no toFloat64OrZero around JSONExtractFloat)
    q_orders = """
    SELECT
      count() AS orders,
      sum(ifNull(JSONExtractFloat(properties, 'orderValue'), 0)) AS revenue
    FROM events_raw
    WHERE tenant_id = %(tenant)s
      AND toDate(time) = %(day)s
      AND name = 'order.completed'
    """
    orders_row = ch.query(q_orders, parameters={"tenant": tenant_id, "day": day}).result_rows[0]
    orders = int(orders_row[0])
    revenue = float(orders_row[1])

    # Anomalies for that day
    q_anoms = """
    SELECT metric, value, baseline, ratio, direction, severity
    FROM anomalies
    WHERE tenant_id = %(tenant)s AND day = %(day)s
    ORDER BY
      multiIf(severity='high', 3, severity='medium', 2, 1) DESC,
      abs(ratio - 1) DESC
    LIMIT 20
    """
    anoms = ch.query(q_anoms, parameters={"tenant": tenant_id, "day": day}).result_rows
    anomalies = [
        {
            "metric": r[0],
            "value": int(r[1]),
            "baseline": float(r[2]),
            "ratio": float(r[3]),
            "direction": r[4],
            "severity": r[5],
        }
        for r in anoms
    ]

    return {
        "tenantId": tenant_id,
        "day": str(day),
        "metrics": {
            "totalEventsTop10": total_events_top10,
            "dau": dau,
            "orders": orders,
            "revenueUsd": revenue,
        },
        "topEvents": top_events,
        "anomalies": anomalies,
    }


def generate_summary_with_openai(facts: Dict[str, Any]) -> DailySummary:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)

    prompt = f"""
You are generating a factual daily activity summary for a product analytics dashboard.
Only use the provided JSON facts. Do not invent numbers.

Constraints:
- Keep bullets <= {SUMMARY_MAX_BULLETS}
- If anomalies exist, mention them prominently.
- Output concise, clear, and actionable.
Facts JSON:
{json.dumps(facts, indent=2)}
""".strip()

    parsed_response = client.responses.parse(
        model=OPENAI_MODEL,
        input=prompt,
        text_format=DailySummary,
    )

    # ✅ Extract the actual DailySummary
    if parsed_response.output_parsed is None:
        raise RuntimeError("No parsed output returned from OpenAI")

    return parsed_response.output_parsed

def write_summary(ch, tenant_id: str, day: date, summary: DailySummary) -> None:
    ch.insert(
        table="activity_summaries",
        data=[
            (
                tenant_id,
                day,
                summary.summary_md,
                json.dumps(summary.highlights.model_dump()),
            )
        ],
        column_names=["tenant_id", "day", "summary_md", "highlights_json"],
    )


def main():
    ch = ch_client()

    # Which day to summarize (default: today)
    day_str = os.getenv("DAY")
    target_day = date.fromisoformat(day_str) if day_str else date.today()

    # Find tenants active that day
    q_tenants = """
    SELECT DISTINCT tenant_id
    FROM events_raw
    WHERE toDate(time) = %(day)s
    LIMIT 1000
    """
    tenants = [r[0] for r in ch.query(q_tenants, parameters={"day": target_day}).result_rows]

    if not tenants:
        print(f"[anomaly-service] no tenants with events on {target_day}")
        return

    # --- anomaly detection (daily counts for ANOMALY_EVENT_NAME over LOOKBACK_DAYS)
    q_counts = """
    SELECT tenant_id, toDate(time) AS day, count() AS value
    FROM events_raw
    WHERE name = %(event)s
      AND day >= today() - %(lookback)s
    GROUP BY tenant_id, day
    ORDER BY tenant_id, day
    """
    rows_raw = ch.query(
        q_counts,
        parameters={"event": ANOMALY_EVENT_NAME, "lookback": LOOKBACK_DAYS},
    ).result_rows
    rows = [{"tenant_id": r[0], "day": r[1], "value": int(r[2])} for r in rows_raw]

    anomalies = detect_daily_count_anomalies(rows)
    write_anomalies(ch, anomalies)
    print(f"[anomaly-service] wrote {len(anomalies)} anomalies for {ANOMALY_EVENT_NAME}")

    # --- summaries per tenant for target_day
    for tenant_id in tenants:
        facts = build_daily_facts(ch, tenant_id, target_day)
        try:
            summary = generate_summary_with_openai(facts)
        except Exception as e:
            print(f"[anomaly-service] OpenAI summary failed for {tenant_id} {target_day}: {e}")
            continue

        write_summary(ch, tenant_id, target_day, summary)
        print(f"[anomaly-service] wrote summary for {tenant_id} {target_day}")

    print("[anomaly-service] done")


if __name__ == "__main__":
    main()