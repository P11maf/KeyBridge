import math
from datetime import datetime, timedelta, timezone

from keybridge.db import query_rows


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _to_dt(v) -> datetime | None:
    """Accepts ISO string or datetime; returns UTC-aware datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        # Handle "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _safe_div(a: float, b: float) -> float | None:
    try:
        if b == 0:
            return None
        return a / b
    except Exception:
        return None


def compute_minute_rollup(
    db_conn,
    minute_start_utc: datetime,
    meter_ids: list[str],
    poll_seconds: float,
) -> dict:
    """
    Computes rollup for [minute_start, minute_start + 60s) from SQLite raw_samples.

    Returns Firestore-ready dict:
      - minuteStart/minuteEnd ISO
      - inlet/outlet aggregates
      - loss aggregate
    """
    minute_start_utc = minute_start_utc.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minute_end_utc = minute_start_utc + timedelta(minutes=1)

    # We store cycleTs as ISO strings. Query by time range on that string (ISO sorts lexicographically).
    start_iso = minute_start_utc.isoformat().replace("+00:00", "Z")
    end_iso = minute_end_utc.isoformat().replace("+00:00", "Z")

    def per_meter(meter_id: str) -> dict:
        rows = query_rows(
            db_conn,
            """
            SELECT cycleTs, flow_raw, total_raw, temp_c
            FROM raw_samples
            WHERE meterId = ?
              AND cycleTs >= ?
              AND cycleTs < ?
            ORDER BY cycleTs ASC
            """,
            (meter_id, start_iso, end_iso),
        )

        if not rows:
            return {
                "ok": False,
                "samples": 0,
                "flow_avg": None,
                "flow_max": None,
                "total_delta": None,
                "temp_avg": None,
            }

        flows = [r["flow_raw"] for r in rows if r["flow_raw"] is not None]
        temps = [r["temp_c"] for r in rows if r["temp_c"] is not None]

        # total_delta = last total_raw - first total_raw (within the minute)
        first_total = None
        last_total = None
        for r in rows:
            if first_total is None and r["total_raw"] is not None:
                first_total = r["total_raw"]
            if r["total_raw"] is not None:
                last_total = r["total_raw"]

        total_delta = None
        if first_total is not None and last_total is not None:
            total_delta = last_total - first_total

        flow_avg = (sum(flows) / len(flows)) if flows else None
        flow_max = max(flows) if flows else None
        temp_avg = (sum(temps) / len(temps)) if temps else None

        return {
            "ok": True,
            "samples": len(rows),
            "flow_avg": flow_avg,
            "flow_max": flow_max,
            "total_delta": total_delta,
            "temp_avg": temp_avg,
        }

    # Build rollup map for meters we care about
    meter_rollups: dict[str, dict] = {}
    for mid in meter_ids:
        meter_rollups[mid] = per_meter(mid)

    inlet = meter_rollups.get("inlet") or per_meter("inlet")
    outlet = meter_rollups.get("outlet") or per_meter("outlet")

    # Loss aggregation (prefer total_delta basis)
    loss_delta = None
    loss_percent = None
    basis = "delta"

    in_delta = inlet.get("total_delta")
    out_delta = outlet.get("total_delta")

    if in_delta is not None and out_delta is not None:
        loss_delta = in_delta - out_delta
        loss_percent = _safe_div(loss_delta, in_delta)
        basis = "delta"
    else:
        # Fallback: estimate per minute from average flow (flow units assumed per minute basis unknown)
        # We approximate "delta" by avg_flow * 60 seconds / 60 = avg_flow (if flow is per-minute),
        # but since we don't know units, just compute relative % from avg_flow.
        in_flow = inlet.get("flow_avg")
        out_flow = outlet.get("flow_avg")
        if in_flow is not None and out_flow is not None:
            loss_delta = in_flow - out_flow
            loss_percent = _safe_div(loss_delta, in_flow)
            basis = "flow_fallback"
        else:
            loss_delta = None
            loss_percent = None
            basis = "unknown"

    # If we can infer expected samples ~ 60 / pollSeconds
    expected = None
    try:
        if poll_seconds and poll_seconds > 0:
            expected = int(round(60.0 / poll_seconds))
    except Exception:
        expected = None

    doc = {
        "minuteStart": start_iso,
        "minuteEnd": end_iso,
        "expectedSamples": expected,
        "inlet": inlet,
        "outlet": outlet,
        "loss": {
            "delta": loss_delta,
            "percent": loss_percent,
            "basis": basis,
        },
    }
    return doc
