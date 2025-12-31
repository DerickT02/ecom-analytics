import json
from datetime import date
from typing import List, Dict, Any

def severity_from_ratio(ratio: float, direction: str) -> str:
    # simple, explainable thresholds
    if direction == "spike":
        if ratio >= 5: return "high"
        if ratio >= 3: return "medium"
        return "low"
    else:  # drop
        if ratio <= 0.2: return "high"
        if ratio <= 0.35: return "medium"
        return "low"

def detect_anomalies(rows: List[Dict[str, Any]], baseline_days: int, spike_ratio: float, drop_ratio: float):
    """
    rows: sorted list of dicts per (tenant_id, day) with 'value'
    We compute baseline as trailing avg of previous baseline_days (excluding current day).
    """
    anomalies = []

    # group by tenant
    by_tenant: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_tenant.setdefault(r["tenant_id"], []).append(r)

    for tenant_id, series in by_tenant.items():
        # series is sorted by day
        values = [s["value"] for s in series]
        days = [s["day"] for s in series]

        for i in range(baseline_days, len(series)):
            current = values[i]
            baseline_window = values[i - baseline_days:i]
            baseline = sum(baseline_window) / max(1, len(baseline_window))

            # avoid divide-by-zero weirdness
            if baseline == 0:
                continue

            ratio = current / baseline

            direction = None
            if ratio >= spike_ratio:
                direction = "spike"
            elif ratio <= drop_ratio:
                direction = "drop"

            if not direction:
                continue

            anomalies.append({
                "tenant_id": tenant_id,
                "day": days[i],
                "value": int(current),
                "baseline": float(baseline),
                "ratio": float(ratio),
                "direction": direction,
                "severity": severity_from_ratio(ratio, direction),
            })

    return anomalies