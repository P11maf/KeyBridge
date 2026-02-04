import time
from datetime import datetime, timezone, timedelta

from keybridge.modbus import read_fd_r_via_mpfen1
from keybridge.simulate import maybe_simulate_outlet

from keybridge.db import (
    enqueue_raw_sample,
    flush_db_if_needed,
    apply_raw_retention,
)

from keybridge.firestore_client import (
    push_meter_latest,
    push_summary_current,
    push_minute_rollup,
    delete_old_minute_rollups,
)


def _now_utc():
    dt = datetime.now(timezone.utc)
    iso_plus00 = dt.isoformat()  # "+00:00"
    iso_z = iso_plus00.replace("+00:00", "Z")
    return dt, iso_plus00, iso_z


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _safe_total_delta(prev_total, cur_total):
    if prev_total is None or cur_total is None:
        return None
    try:
        delta = cur_total - prev_total
        if delta < 0:
            return None
        # sanity cap per second (tune later)
        if delta > 10_000_000:
            return None
        return delta
    except Exception:
        return None


def build_summary_current(cycle_ts_z: str, inlet_doc: dict, outlet_doc: dict) -> dict:
    inlet_ok = bool(inlet_doc.get("ok", False)) and inlet_doc.get("flow_raw") is not None
    outlet_ok = bool(outlet_doc.get("ok", False)) and outlet_doc.get("flow_raw") is not None
    sync_ok = inlet_doc.get("cycleTs") == outlet_doc.get("cycleTs")

    inlet_delta = inlet_doc.get("total_delta")
    outlet_delta = outlet_doc.get("total_delta")

    loss_delta = None
    loss_percent = None
    basis = "delta"

    if inlet_delta is not None and outlet_delta is not None:
        loss_delta = inlet_delta - outlet_delta
        loss_percent = (loss_delta / inlet_delta) if inlet_delta else None
        basis = "delta"
    else:
        in_flow = inlet_doc.get("flow_raw")
        out_flow = outlet_doc.get("flow_raw")
        if in_flow is not None and out_flow is not None:
            loss_delta = in_flow - out_flow
            loss_percent = (loss_delta / in_flow) if in_flow else None
            basis = "flow_fallback"
        else:
            basis = "unknown"

    return {
        "cycleTs": cycle_ts_z,
        "quality": {"inlet_ok": inlet_ok, "outlet_ok": outlet_ok, "sync": sync_ok},
        "inlet": {
            "ok": inlet_ok,
            "flow_raw": inlet_doc.get("flow_raw"),
            "total_raw": inlet_doc.get("total_raw"),
            "delta": inlet_doc.get("total_delta"),
            "temp_c": inlet_doc.get("temp_c"),
            "source": inlet_doc.get("source"),
            "error": inlet_doc.get("error"),
        },
        "outlet": {
            "ok": outlet_ok,
            "flow_raw": outlet_doc.get("flow_raw"),
            "total_raw": outlet_doc.get("total_raw"),
            "delta": outlet_doc.get("total_delta"),
            "temp_c": outlet_doc.get("temp_c"),
            "source": outlet_doc.get("source"),
            "error": outlet_doc.get("error"),
        },
        "loss": {"delta": loss_delta, "percent": loss_percent, "basis": basis},
    }


def _compute_minute_rollup_from_sqlite(db_conn, minute_start_utc: datetime, poll_seconds: float) -> dict:
    """
    Computes rollup from YOUR sqlite schema:
      raw_samples(cycle_ts_utc, meter_id, flow_raw, total_raw, temp_c)
    """
    minute_start_utc = minute_start_utc.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minute_end_utc = minute_start_utc + timedelta(minutes=1)

    start_iso = minute_start_utc.isoformat()
    end_iso = minute_end_utc.isoformat()

    def per_meter(meter_id: str) -> dict:
        cur = db_conn.execute(
            """
            SELECT cycle_ts_utc, flow_raw, total_raw, temp_c
            FROM raw_samples
            WHERE meter_id = ?
              AND cycle_ts_utc >= ?
              AND cycle_ts_utc < ?
            ORDER BY cycle_ts_utc ASC
            """,
            (meter_id, start_iso, end_iso),
        )
        rows = cur.fetchall() or []
        if not rows:
            return {
                "ok": False,
                "samples": 0,
                "flow_avg": None,
                "flow_max": None,
                "total_delta": None,
                "temp_avg": None,
            }

        flows = [r[1] for r in rows if r[1] is not None]
        temps = [r[3] for r in rows if r[3] is not None]

        first_total = None
        last_total = None
        for r in rows:
            t = r[2]
            if first_total is None and t is not None:
                first_total = t
            if t is not None:
                last_total = t

        total_delta = None
        if first_total is not None and last_total is not None:
            td = last_total - first_total
            total_delta = td if td >= 0 else None

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

    inlet = per_meter("inlet")
    outlet = per_meter("outlet")

    # Loss
    loss_delta = None
    loss_percent = None
    basis = "delta"

    in_delta = inlet.get("total_delta")
    out_delta = outlet.get("total_delta")

    if in_delta is not None and out_delta is not None:
        loss_delta = in_delta - out_delta
        loss_percent = (loss_delta / in_delta) if in_delta else None
        basis = "delta"
    else:
        in_flow = inlet.get("flow_avg")
        out_flow = outlet.get("flow_avg")
        if in_flow is not None and out_flow is not None:
            loss_delta = in_flow - out_flow
            loss_percent = (loss_delta / in_flow) if in_flow else None
            basis = "flow_fallback"
        else:
            basis = "unknown"

    expected = None
    try:
        if poll_seconds and poll_seconds > 0:
            expected = int(round(60.0 / poll_seconds))
    except Exception:
        expected = None

    start_z = start_iso.replace("+00:00", "Z")
    end_z = end_iso.replace("+00:00", "Z")

    return {
        "minuteStart": start_z,
        "minuteEnd": end_z,
        "expectedSamples": expected,
        "inlet": inlet,
        "outlet": outlet,
        "loss": {"delta": loss_delta, "percent": loss_percent, "basis": basis},
    }


def poll_loop(db_conn, fs, latest: dict, recording_state: dict, last_totals: dict, get_config_live):
    """
    Main polling loop (thread target) for app.py.
    Populates:
      - latest.updatedUtc, latest.summary, latest.devices (for dashboard.html)
      - debug_* fields (for /debug)
      - minute rollups + retention
    """
    latest["polling"] = True
    latest["lastPollError"] = None

    last_seen_minute = None
    last_retention_run = 0.0

    while True:
        cfg = get_config_live()

        tenant_id = cfg.get("tenantId")
        site_id = cfg.get("siteId")
        poll_seconds = float(cfg.get("pollSeconds", 1.0))

        retention_cfg = cfg.get("retention", {}) or {}
        minute_rollups_enabled = bool(retention_cfg.get("minute_rollups_enabled", True))
        minute_rollups_retention_enabled = bool(retention_cfg.get("minute_rollups_retention_enabled", True))
        minute_rollups_days = int(retention_cfg.get("minute_rollups_days", 90))

        devices = cfg.get("devices", []) or []
        inlet_dev = next((d for d in devices if d.get("enabled") and d.get("meterId") == "inlet"), None)

        dt_utc, cycle_ts_plus00, cycle_ts_z = _now_utc()

        # Debug always-visible keys
        latest["debug_cycleTs"] = cycle_ts_z
        latest["debug_inlet_ip"] = inlet_dev.get("ip") if inlet_dev else None
        latest["debug_inlet_port"] = int(inlet_dev.get("port", 502)) if inlet_dev else None
        latest["debug_sim_enabled"] = bool(((cfg.get("simulate", {}) or {}).get("outlet", {}) or {}).get("enabled", False))
        latest["debug_poll_seconds"] = poll_seconds

        try:
            # ---------------------------
            # Read inlet (Modbus)
            # ---------------------------
            inlet_doc = {"cycleTs": cycle_ts_z, "source": "mp-fen1", "ok": False}

            if not inlet_dev:
                result = {"ok": False, "error": "No inlet device configured/enabled"}
            else:
                ip = inlet_dev.get("ip")
                port = int(inlet_dev.get("port", 502))
                result = read_fd_r_via_mpfen1(ip, port)

            # Capture modbus result into debug
            latest["debug_modbus_ok"] = bool(result.get("ok", False))
            latest["debug_modbus_error"] = result.get("error")
            latest["debug_modbus_keys"] = sorted(list(result.keys()))

            inlet_doc.update(result)
            inlet_doc["cycleTs"] = cycle_ts_z
            inlet_doc["source"] = "mp-fen1"
            inlet_doc["ok"] = bool(result.get("ok", False))
            if not inlet_doc["ok"]:
                inlet_doc["error"] = result.get("error", "Unknown Modbus error")

            # total_delta
            if inlet_doc.get("ok") and inlet_doc.get("total_raw") is not None:
                prev = last_totals.get("inlet")
                cur = inlet_doc.get("total_raw")
                inlet_doc["total_delta"] = _safe_total_delta(prev, cur)
                last_totals["inlet"] = cur
            else:
                inlet_doc["total_delta"] = None

            # ---------------------------
            # Outlet (simulated for now)
            # ---------------------------
            outlet_doc = maybe_simulate_outlet(cfg, cycle_ts_z, inlet_doc) or {}
            outlet_doc.setdefault("cycleTs", cycle_ts_z)
            outlet_doc.setdefault("source", outlet_doc.get("source", "simulated"))
            outlet_doc.setdefault("ok", bool(outlet_doc.get("ok", True)))

            if outlet_doc.get("total_raw") is not None:
                prev = last_totals.get("outlet")
                cur = outlet_doc.get("total_raw")
                outlet_doc["total_delta"] = _safe_total_delta(prev, cur)
                last_totals["outlet"] = cur
            else:
                outlet_doc["total_delta"] = None

            # ---------------------------
            # Debug: why summary may be missing
            # ---------------------------
            latest["debug_inlet_doc_ok"] = bool(inlet_doc.get("ok", False))
            latest["debug_inlet_flow_raw"] = inlet_doc.get("flow_raw")
            latest["debug_outlet_doc_ok"] = bool(outlet_doc.get("ok", False))
            latest["debug_outlet_flow_raw"] = outlet_doc.get("flow_raw")
            latest["debug_summary_ready"] = (
                bool(inlet_doc.get("ok", False)) and inlet_doc.get("flow_raw") is not None
                and bool(outlet_doc.get("ok", False)) and outlet_doc.get("flow_raw") is not None
            )

            # ---------------------------
            # SQLite raw samples (batch commit)
            # ---------------------------
            def to_row(meter_id: str, d: dict) -> dict:
                return {
                    "cycle_ts_utc": cycle_ts_plus00,
                    "meter_id": meter_id,
                    "flow_raw": d.get("flow_raw"),
                    "total_raw": d.get("total_raw"),
                    "temp_raw": d.get("temp_raw"),
                    "temp_c": d.get("temp_c"),
                    "stability": d.get("stability"),
                    "module_status": d.get("module_status"),
                    "data_status": d.get("data_status"),
                    "diag_info": d.get("diag_info"),
                    "event0_code": d.get("event0_code"),
                    "data_valid": d.get("data_valid", True),
                }

            enqueue_raw_sample(to_row("inlet", inlet_doc))
            enqueue_raw_sample(to_row("outlet", outlet_doc))
            flush_db_if_needed(db_conn, cfg, latest)
            apply_raw_retention(db_conn, cfg, cycle_ts_z, latest)

            # ---------------------------
            # Dashboard runtime state (THIS is what your dashboard.html expects)
            # ---------------------------
            summary = build_summary_current(cycle_ts_z, inlet_doc, outlet_doc)

            latest["updatedUtc"] = cycle_ts_z
            latest["cycleTs"] = cycle_ts_z
            latest["summary"] = summary

            latest["devices"] = {
                "inlet": {
                    "meterId": "inlet",
                    "label": (inlet_dev.get("label") if inlet_dev else "Inlet"),
                    "ip": (inlet_dev.get("ip") if inlet_dev else None),
                    "port": (int(inlet_dev.get("port", 502)) if inlet_dev else None),
                    **inlet_doc,
                },
                "outlet": {
                    "meterId": "outlet",
                    "label": "Outlet",
                    "ip": None,
                    "port": None,
                    **outlet_doc,
                },
            }

            # ---------------------------
            # Firestore writes (live docs + summary/current)
            # ---------------------------
            if fs:
                push_meter_latest(fs, tenant_id, site_id, "inlet", inlet_doc)
                push_meter_latest(fs, tenant_id, site_id, "outlet", outlet_doc)
                push_summary_current(fs, tenant_id, site_id, summary)
                latest["lastFirestoreWriteUtc"] = cycle_ts_z

            # ---------------------------
            # Minute rollups + retention
            # ---------------------------
            if fs and minute_rollups_enabled:
                cur_min = _floor_to_minute(dt_utc)
                if last_seen_minute is None:
                    last_seen_minute = cur_min

                if cur_min > last_seen_minute:
                    minute_start = last_seen_minute
                    rollup = _compute_minute_rollup_from_sqlite(db_conn, minute_start, poll_seconds=poll_seconds)
                    push_minute_rollup(fs, tenant_id, site_id, rollup["minuteStart"], rollup)
                    last_seen_minute = cur_min

                now_s = time.time()
                if minute_rollups_retention_enabled and (now_s - last_retention_run) > 3600:
                    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=minute_rollups_days)
                    cutoff_iso = cutoff_dt.isoformat().replace("+00:00", "Z")
                    try:
                        deleted = delete_old_minute_rollups(fs, tenant_id, site_id, cutoff_iso=cutoff_iso, limit=200)
                        latest["minuteRollupRetentionDeleted"] = deleted
                        latest["minuteRollupRetentionCutoff"] = cutoff_iso
                        latest["minuteRollupRetentionError"] = None
                    except Exception as e:
                        latest["minuteRollupRetentionError"] = str(e)
                    last_retention_run = now_s

            latest["lastCycleTs"] = cycle_ts_z
            latest["lastPollError"] = None

        except Exception as e:
            latest["lastPollError"] = str(e)

        time.sleep(poll_seconds)
