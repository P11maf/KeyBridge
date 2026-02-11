import math
from datetime import datetime, timezone

# Per-meter sim state so each simulated meter accumulates its own total.
_SIM_STATE: dict[str, dict] = {}


def _state_for(meter_id: str) -> dict:
    st = _SIM_STATE.get(meter_id)
    if st is None:
        st = {"last_ts": None, "total_raw": 0}
        _SIM_STATE[meter_id] = st
    return st


def simulate_meter(
    *,
    meter_id: str,
    label: str,
    cycle_ts_z: str,
    base_doc: dict | None = None,
    loss_factor: float = 0.98,
) -> dict:
    """
    Generate simulated meter readings.

    - If base_doc provided, mirrors base flow * loss_factor (useful for outlet)
    - Else generates a synthetic wave around ~60M raw units.
    """

    # Parse time
    try:
        dt = datetime.fromisoformat(cycle_ts_z.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)

    t = dt.timestamp()

    # Mirror base flow if present, otherwise wave
    base_flow = None
    base_temp = None
    if base_doc:
        try:
            if base_doc.get("flow_raw") is not None:
                base_flow = float(base_doc.get("flow_raw"))
        except Exception:
            base_flow = None
        try:
            if base_doc.get("temp_c") is not None:
                base_temp = float(base_doc.get("temp_c"))
        except Exception:
            base_temp = None

    if base_flow is None:
        flow_raw = 60_000_000 + int(10_000_000 * math.sin(t / 10.0))
    else:
        flow_raw = int(base_flow * loss_factor)

    # Temperature
    if base_temp is None:
        temp_c = 10.0 + (1.0 * math.sin(t / 60.0))
    else:
        temp_c = float(base_temp)

    temp_raw = int(round(temp_c * 10.0))

    # Accumulate total per meter
    st = _state_for(meter_id)
    last_ts = st.get("last_ts")
    st["last_ts"] = t

    if last_ts is None:
        dt_s = 1.0
    else:
        dt_s = max(0.1, min(5.0, t - last_ts))

    # crude accumulation (good enough for proving pipeline)
    st["total_raw"] = int((st.get("total_raw") or 0) + (flow_raw * dt_s / 60.0))
    total_raw = int(st["total_raw"])

    return {
        "cycleTs": cycle_ts_z,
        "meterId": meter_id,
        "label": label,
        "source": "simulated",
        "ok": True,
        "error": None,
        "flow_raw": flow_raw,
        "total_raw": total_raw,
        "temp_raw": temp_raw,
        "temp_c": temp_c,
        "stability": 100,
        "module_status": 0,
        "data_status": 0,
        "diag_info": 0,
        "event0_code": 0,
        "data_valid": True,
    }
