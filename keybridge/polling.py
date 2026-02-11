import time
from datetime import datetime, timezone, timedelta

from keybridge.modbus import read_fd_r_via_mpfen1
from keybridge.simulate import simulate_meter

from keybridge.db import (
    enqueue_raw_sample,
    flush_db_if_needed,
    apply_raw_retention,
)

from keybridge.firestore_client import (
    push_meter_latest_guarded,
    push_summary_current,
    push_minute_rollup,
    delete_old_minute_rollups,
    get_meter_latest,
    push_gateway_heartbeat,
)


def _now_utc():
    dt = datetime.now(timezone.utc)
    iso_plus00 = dt.isoformat()
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
            "gatewayId": inlet_doc.get("gatewayId"),
        },
        "outlet": {
            "ok": outlet_ok,
            "flow_raw": outlet_doc.get("flow_raw"),
            "total_raw": outlet_doc.get("total_raw"),
            "delta": outlet_doc.get("total_delta"),
            "temp_c": outlet_doc.get("temp_c"),
            "source": outlet_doc.get("source"),
            "error": outlet_doc.get("error"),
            "gatewayId": outlet_doc.get("gatewayId"),
        },
        "loss": {"delta": loss_delta, "percent": loss_percent, "basis": basis},
    }


def _compute_minute_rollup_from_sqlite(
    db_conn,
    minute_start_utc: datetime,
    poll_seconds: float,
    meter_ids: list[str],
) -> dict:
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

    meters = {mid: per_meter(mid) for mid in meter_ids}

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
        "meters": meters,
    }


def _choose_display_inlet_outlet(cfg: dict, meter_docs: dict) -> tuple[dict, dict]:
    devices = cfg.get("devices", []) or []
    rules = cfg.get("summaryRules", {}) or {}

    inlet_id = (rules.get("inletMeterId") or "").strip()
    outlet_id = (rules.get("outletMeterId") or "").strip()

    if not inlet_id:
        role_in = next((d for d in devices if d.get("enabled") and (d.get("role") == "inlet")), None)
        inlet_id = (role_in.get("meterId") if role_in else "") or ""

    if not outlet_id:
        role_out = next((d for d in devices if d.get("enabled") and (d.get("role") == "outlet")), None)
        outlet_id = (role_out.get("meterId") if role_out else "") or ""

    enabled_ids = [d.get("meterId") for d in devices if d.get("enabled") and d.get("meterId")]
    enabled_ids = [x for x in enabled_ids if x in meter_docs]

    if not inlet_id and enabled_ids:
        inlet_id = enabled_ids[0]
    if not outlet_id and len(enabled_ids) > 1:
        outlet_id = enabled_ids[1]

    inlet_doc = meter_docs.get(inlet_id) or {"cycleTs": meter_docs.get("_cycleTs"), "ok": False, "error": "No inlet selected"}
    outlet_doc = meter_docs.get(outlet_id) or {"cycleTs": meter_docs.get("_cycleTs"), "ok": False, "error": "No outlet selected"}

    return inlet_doc, outlet_doc


def poll_loop(db_conn, fs, latest: dict, recording_state: dict, last_totals: dict, get_config_live):
    latest["polling"] = True
    latest["lastPollError"] = None

    last_seen_minute = None
    last_retention_run = 0.0
    last_heartbeat = 0.0

    while True:
        cfg = get_config_live()

        tenant_id = (cfg.get("tenantId") or "").strip()
        site_id = (cfg.get("siteId") or "").strip()
        gateway_id = (cfg.get("gatewayId") or "").strip() or "gateway-unknown"
        mode = (cfg.get("mode") or "gateway").strip().lower()
        poll_seconds = float(cfg.get("pollSeconds", 1.0))

        retention_cfg = cfg.get("retention", {}) or {}
        minute_rollups_enabled = bool(retention_cfg.get("minute_rollups_enabled", True))
        minute_rollups_retention_enabled = bool(retention_cfg.get("minute_rollups_retention_enabled", True))
        minute_rollups_days = int(retention_cfg.get("minute_rollups_days", 90))

        summary_rules = cfg.get("summaryRules", {}) or {}
        summary_enabled = bool(summary_rules.get("enabled", False))

        sim_cfg = cfg.get("simulate", {}) or {}
        sim_enabled = bool(sim_cfg.get("enabled", False))

        devices = cfg.get("devices", []) or []
        # ✅ allow simulated devices without IP
        enabled_devices = [d for d in devices if d.get("enabled") and d.get("meterId")]

        dt_utc, cycle_ts_plus00, cycle_ts_z = _now_utc()

        latest["debug_cycleTs"] = cycle_ts_z
        latest["debug_poll_seconds"] = poll_seconds
        latest["debug_mode"] = mode
        latest["debug_gatewayId"] = gateway_id
        latest["debug_devices_enabled"] = [d.get("meterId") for d in enabled_devices]

        try:
            meter_docs: dict[str, dict] = {"_cycleTs": cycle_ts_z}

            # HEARTBEAT
            if fs:
                now_s = time.time()
                if (now_s - last_heartbeat) > 15.0:
                    try:
                        push_gateway_heartbeat(
                            fs,
                            tenant_id=tenant_id,
                            gateway_id=gateway_id,
                            site_id=site_id,
                            status="ok",
                            version="vNext",
                            mode=mode,
                            extra={"deviceCount": len(enabled_devices)},
                        )
                        latest["heartbeatOk"] = True
                        latest["heartbeatUtc"] = cycle_ts_z
                    except Exception as e:
                        latest["heartbeatOk"] = False
                        latest["heartbeatError"] = str(e)
                    last_heartbeat = now_s

            # GATEWAY MODE
            if mode in ("gateway", "both"):
                latest.setdefault("debug_modbus", {})
                latest.setdefault("firestoreMeterWrites", {})

                for d in enabled_devices:
                    meter_id = (d.get("meterId") or "").strip()
                    label = (d.get("label") or "").strip() or meter_id
                    ip = (d.get("ip") or "").strip()
                    port = int(d.get("port", 502))
                    profile = (d.get("profile") or "fd-r-v1").strip()
                    source = (d.get("source") or "mp-fen1").strip()

                    latest["debug_modbus"][meter_id] = {"ip": ip or None, "port": port, "ok": None, "error": None}

                    doc = {
                        "cycleTs": cycle_ts_z,
                        "meterId": meter_id,
                        "label": label,
                        "source": source,
                        "profile": profile,
                        "gatewayId": gateway_id,
                        "ok": False,
                    }

                    # ✅ NEW: per-device simulation support
                    if source == "simulated":
                        if not sim_enabled:
                            result = {"ok": False, "error": "Simulation disabled (set simulate_enabled=true)."}
                        else:
                            result = simulate_meter(
                                meter_id=meter_id,
                                label=label,
                                cycle_ts_z=cycle_ts_z,
                                base_doc=None,
                                loss_factor=0.98,
                            )
                    else:
                        # real modbus
                        if not ip:
                            result = {"ok": False, "error": "Missing IP address"}
                        elif profile != "fd-r-v1":
                            result = {"ok": False, "error": f"Unsupported profile: {profile}"}
                        else:
                            result = read_fd_r_via_mpfen1(ip, port)

                    doc.update(result)
                    doc["ok"] = bool(doc.get("ok", False))
                    if not doc["ok"]:
                        doc["error"] = doc.get("error") or "Unknown error"

                    if doc.get("ok") and doc.get("total_raw") is not None:
                        prev = last_totals.get(meter_id)
                        cur = doc.get("total_raw")
                        doc["total_delta"] = _safe_total_delta(prev, cur)
                        last_totals[meter_id] = cur
                    else:
                        doc["total_delta"] = None

                    meter_docs[meter_id] = doc

                    enqueue_raw_sample({
                        "cycle_ts_utc": cycle_ts_plus00,
                        "meter_id": meter_id,
                        "flow_raw": doc.get("flow_raw"),
                        "total_raw": doc.get("total_raw"),
                        "temp_raw": doc.get("temp_raw"),
                        "temp_c": doc.get("temp_c"),
                        "stability": doc.get("stability"),
                        "module_status": doc.get("module_status"),
                        "data_status": doc.get("data_status"),
                        "diag_info": doc.get("diag_info"),
                        "event0_code": doc.get("event0_code"),
                        "data_valid": doc.get("data_valid", True),
                    })

                    if fs:
                        ok_write = push_meter_latest_guarded(
                            fs,
                            tenant_id=tenant_id,
                            site_id=site_id,
                            meter_id=meter_id,
                            payload=doc,
                            gateway_id=gateway_id,
                            latest=latest,
                        )
                        latest["firestoreMeterWrites"][meter_id] = bool(ok_write)

                    latest["debug_modbus"][meter_id]["ok"] = bool(doc.get("ok", False))
                    latest["debug_modbus"][meter_id]["error"] = doc.get("error")

                flush_db_if_needed(db_conn, cfg, latest)
                apply_raw_retention(db_conn, cfg, cycle_ts_z, latest)

            # AGGREGATOR MODE (optional)
            inlet_for_summary = None
            outlet_for_summary = None

            if mode in ("aggregator", "both") and fs and tenant_id and site_id and summary_enabled:
                inlet_id = (summary_rules.get("inletMeterId") or "").strip()
                outlet_id = (summary_rules.get("outletMeterId") or "").strip()
                max_age_s = int(summary_rules.get("maxAgeSeconds", 10))
                require_sync = bool(summary_rules.get("requireSync", False))

                inlet_for_summary = get_meter_latest(fs, tenant_id, site_id, inlet_id) if inlet_id else None
                outlet_for_summary = get_meter_latest(fs, tenant_id, site_id, outlet_id) if outlet_id else None

                def _age_ok(doc: dict | None) -> bool:
                    if not doc:
                        return False
                    ts = doc.get("cycleTs")
                    if not ts:
                        return False
                    try:
                        dtp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        age = (datetime.now(timezone.utc) - dtp).total_seconds()
                        return age <= max_age_s
                    except Exception:
                        return False

                inlet_age_ok = _age_ok(inlet_for_summary)
                outlet_age_ok = _age_ok(outlet_for_summary)
                sync_ok = True
                if require_sync and inlet_for_summary and outlet_for_summary:
                    sync_ok = inlet_for_summary.get("cycleTs") == outlet_for_summary.get("cycleTs")

                latest["aggregator"] = {
                    "enabled": True,
                    "inletMeterId": inlet_id,
                    "outletMeterId": outlet_id,
                    "inlet_age_ok": inlet_age_ok,
                    "outlet_age_ok": outlet_age_ok,
                    "sync_ok": sync_ok,
                    "maxAgeSeconds": max_age_s,
                    "requireSync": require_sync,
                }

                if inlet_for_summary and outlet_for_summary and inlet_age_ok and outlet_age_ok and sync_ok:
                    sum_cycle = inlet_for_summary.get("cycleTs") or cycle_ts_z
                    summary = build_summary_current(sum_cycle, inlet_for_summary, outlet_for_summary)
                    push_summary_current(fs, tenant_id, site_id, summary)
                    latest["lastFirestoreWriteUtc"] = cycle_ts_z
                    latest["summaryWritten"] = True
                else:
                    latest["summaryWritten"] = False

            inlet_doc, outlet_doc = _choose_display_inlet_outlet(cfg, meter_docs)
            summary_for_ui = build_summary_current(inlet_doc.get("cycleTs") or cycle_ts_z, inlet_doc, outlet_doc)

            latest["updatedUtc"] = cycle_ts_z
            latest["cycleTs"] = cycle_ts_z
            latest["summary"] = summary_for_ui

            def _dev_meta(mid: str) -> dict:
                dev = next((d for d in devices if d.get("meterId") == mid), None)
                if not dev:
                    return {"meterId": mid, "label": mid, "ip": None, "port": None}
                return {
                    "meterId": mid,
                    "label": dev.get("label") or mid,
                    "ip": dev.get("ip"),
                    "port": int(dev.get("port", 502)) if dev.get("port") is not None else None,
                }

            inlet_mid = str(inlet_doc.get("meterId") or "inlet")
            outlet_mid = str(outlet_doc.get("meterId") or "outlet")

            latest["devices"] = {
                "inlet": {**_dev_meta(inlet_mid), **inlet_doc},
                "outlet": {**_dev_meta(outlet_mid), **outlet_doc},
            }

            latest["meters"] = {k: v for k, v in meter_docs.items() if not k.startswith("_")}

            # MINUTE ROLLUPS
            if fs and minute_rollups_enabled:
                cur_min = _floor_to_minute(dt_utc)
                if last_seen_minute is None:
                    last_seen_minute = cur_min

                if cur_min > last_seen_minute:
                    minute_start = last_seen_minute

                    meter_ids_for_rollup = []
                    for d in devices:
                        mid = (d.get("meterId") or "").strip()
                        if mid:
                            meter_ids_for_rollup.append(mid)
                    for mid in latest.get("meters", {}).keys():
                        if mid and mid not in meter_ids_for_rollup:
                            meter_ids_for_rollup.append(mid)

                    rollup = _compute_minute_rollup_from_sqlite(
                        db_conn,
                        minute_start,
                        poll_seconds=poll_seconds,
                        meter_ids=meter_ids_for_rollup,
                    )
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
