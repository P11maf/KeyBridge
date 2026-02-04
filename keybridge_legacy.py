import json
import time
import threading
import os
import sqlite3
from datetime import datetime, timezone, timedelta

from pymodbus.client import ModbusTcpClient
from flask import Flask, render_template, request, redirect, url_for

import firebase_admin
from firebase_admin import credentials, firestore

from commands import process_commands
from recorder import start_recording, stop_recording  # imported for completeness (commands uses these)


# ============================================================
# Constants / Modbus registers
# ============================================================
REG_FLOW_U32 = 7142
REG_TOTAL_U32 = 7144
REG_TEMP_I16 = 7146
REG_STAB_U16 = 7154
UNIT_ID = 1

REG_MODULE_STATUS = 6000
REG_DATA_STATUS = 6001
REG_DIAG_INFO = 6019
REG_EVENT0_CATEGORY = 6020
REG_EVENT0_CODE = 6021


# ============================================================
# Paths
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "keybridge.sqlite")


# ============================================================
# Global config (live)
# ============================================================
CONFIG_LOCK = threading.Lock()
CONFIG = None


# ============================================================
# Recording runtime state (Phase 1 authority)
# ============================================================
RECORDING_LOCK = threading.Lock()
recording_state = {}  # meterId -> {is_on, session_id, started_utc}


# ============================================================
# Totalizer state for delta calc (Phase 2 authority)
# ============================================================
TOTALS_LOCK = threading.Lock()
last_totals = {}  # meterId -> last_total_raw


# ============================================================
# DB write batching state
# ============================================================
DB_LOCK = threading.Lock()
pending_raw_rows = []
last_commit_monotonic = time.monotonic()


# ============================================================
# Dashboard state
# ============================================================
latest = {
    "updatedUtc": None,
    "cycleTs": None,
    "devices": {},
    "summary": None,
    "firestoreOk": False,
    "lastFirestoreError": None,
    "lastFirestoreWriteUtc": None,
    "localDbOk": False,
    "lastLocalDbError": None,
}


# ============================================================
# Config helpers
# ============================================================
def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def set_config_live(cfg):
    global CONFIG
    with CONFIG_LOCK:
        CONFIG = cfg


def get_config_live():
    with CONFIG_LOCK:
        return json.loads(json.dumps(CONFIG))


# ============================================================
# SQLite init + pragmas
# ============================================================
def init_db(cfg: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    # Apply pragmas (Pi friendly)
    try:
        pragmas = (cfg.get("sqlite", {}) or {}).get("pragmas", {}) or {}
        journal = pragmas.get("journal_mode")
        sync = pragmas.get("synchronous")
        if journal:
            conn.execute(f"PRAGMA journal_mode={journal}")
        if sync:
            conn.execute(f"PRAGMA synchronous={sync}")
    except Exception:
        # Don't crash if pragma fails; still run
        pass

    # Phase 1: recording sessions
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recording_sessions (
            session_id TEXT PRIMARY KEY,
            meter_id TEXT NOT NULL,
            started_utc TEXT NOT NULL,
            ended_utc TEXT,
            started_by TEXT,
            ended_by TEXT
        )
    """)

    # Phase 2: raw samples every poll cycle (authoritative local history)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_ts_utc TEXT NOT NULL,
            meter_id TEXT NOT NULL,

            flow_raw INTEGER,
            total_raw INTEGER,
            temp_raw INTEGER,
            temp_c REAL,
            stability INTEGER,

            module_status INTEGER,
            data_status INTEGER,
            diag_info INTEGER,
            event0_code INTEGER,

            data_valid INTEGER
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_samples_meter_cycle
        ON raw_samples (meter_id, cycle_ts_utc)
    """)

    conn.commit()
    return conn


# ============================================================
# Firestore init
# ============================================================
def init_firestore(cfg: dict):
    # keep compatibility with your existing file name
    key_path = os.path.join(BASE_DIR, "serviceAccountKey.json")
    # allow override via config
    service_path = (cfg.get("firestore", {}) or {}).get("serviceAccountPath")
    if service_path:
        key_path = os.path.join(BASE_DIR, service_path)

    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Missing service account JSON: {key_path}")

    if not firebase_admin._apps:
        cred = credentials.Certificate(key_path)
        firebase_admin.initialize_app(cred)

    return firestore.client()


# Firestore client created in main after loading cfg
fs = None


# ============================================================
# Firestore writers
# ============================================================
def push_meter_latest(tenant_id, site_id, meter_id, payload):
    global fs
    if fs is None:
        return
    fs.collection("tenants").document(tenant_id) \
        .collection("flow_sites").document(site_id) \
        .collection("meters").document(meter_id) \
        .set(payload, merge=True)


def push_summary(tenant_id, site_id, payload):
    global fs
    if fs is None:
        return
    fs.collection("tenants").document(tenant_id) \
        .collection("flow_sites").document(site_id) \
        .collection("summary").document("current") \
        .set(payload, merge=True)


# ============================================================
# Modbus helpers
# ============================================================
def read_u32(client, address):
    rr = client.read_holding_registers(address, 2, slave=UNIT_ID)
    if rr.isError():
        raise RuntimeError(rr)
    return (rr.registers[0] << 16) | rr.registers[1]


def read_i16(client, address):
    rr = client.read_holding_registers(address, 1, slave=UNIT_ID)
    if rr.isError():
        raise RuntimeError(rr)
    val = rr.registers[0]
    return val - 65536 if val >= 32768 else val


def read_u16(client, address):
    rr = client.read_holding_registers(address, 1, slave=UNIT_ID)
    if rr.isError():
        raise RuntimeError(rr)
    return rr.registers[0]


# ============================================================
# Delta helper (Phase 2)
# ============================================================
def calc_total_delta(meter_id: str, total_now: int):
    with TOTALS_LOCK:
        prev = last_totals.get(meter_id)
        last_totals[meter_id] = total_now

    if prev is None:
        return None

    delta = total_now - prev
    if delta < 0:
        return None
    return delta


# ============================================================
# DB batching + retention helpers
# ============================================================
def enqueue_raw_sample(row: dict):
    global pending_raw_rows
    with DB_LOCK:
        pending_raw_rows.append(row)


def flush_db_if_needed(db_conn, cfg: dict):
    """
    Commit raw_samples in batches every N seconds.
    """
    global pending_raw_rows, last_commit_monotonic

    commit_every = float((cfg.get("sqlite", {}) or {}).get("commit_every_seconds", 5))
    now_mono = time.monotonic()
    if (now_mono - last_commit_monotonic) < commit_every:
        return

    with DB_LOCK:
        rows = pending_raw_rows
        pending_raw_rows = []

    if not rows:
        last_commit_monotonic = now_mono
        return

    try:
        with db_conn:
            db_conn.executemany("""
                INSERT INTO raw_samples (
                    cycle_ts_utc, meter_id,
                    flow_raw, total_raw, temp_raw, temp_c, stability,
                    module_status, data_status, diag_info, event0_code,
                    data_valid
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    r["cycle_ts_utc"], r["meter_id"],
                    r.get("flow_raw"), r.get("total_raw"), r.get("temp_raw"), r.get("temp_c"), r.get("stability"),
                    r.get("module_status"), r.get("data_status"), r.get("diag_info"), r.get("event0_code"),
                    1 if r.get("data_valid", True) else 0
                )
                for r in rows
            ])
        latest["localDbOk"] = True
        latest["lastLocalDbError"] = None
    except Exception as e:
        latest["localDbOk"] = False
        latest["lastLocalDbError"] = str(e)

    last_commit_monotonic = now_mono


def apply_raw_retention(db_conn, cfg: dict, cycle_ts_iso: str):
    """
    Keep only last N minutes of raw_samples if enabled.
    Runs cheaply once per ~minute (guarded in poll loop).
    """
    retention = cfg.get("retention", {}) or {}
    if not retention.get("raw_seconds_enabled", True):
        return

    window_min = int(retention.get("raw_seconds_window_minutes", 60))
    # ISO strings compare lexicographically if same format; we use datetime to be safe.
    cutoff = datetime.fromisoformat(cycle_ts_iso.replace("Z", "+00:00")) - timedelta(minutes=window_min)
    cutoff_iso = cutoff.astimezone(timezone.utc).isoformat()

    try:
        with db_conn:
            db_conn.execute("DELETE FROM raw_samples WHERE cycle_ts_utc < ?", (cutoff_iso,))
    except Exception as e:
        latest["localDbOk"] = False
        latest["lastLocalDbError"] = str(e)


# ============================================================
# Poll loop (Phase 2 + retention + batching)
# ============================================================
def poll_loop(db_conn):
    last_retention_mono = 0.0

    while True:
        cfg = get_config_live()
        tenant_id = cfg["tenantId"]
        site_id = cfg["siteId"]
        poll_s = float(cfg.get("pollSeconds", 1))

        mirror_enabled, mirror_from, sim_out_cfg = should_mirror_outlet(cfg)
        sim_out_meter_id = (sim_out_cfg.get("meterId", "outlet") if mirror_enabled else None)

        # One timestamp shared across all device reads in this cycle
        cycle_ts = datetime.now(timezone.utc).isoformat()
        latest["updatedUtc"] = cycle_ts
        latest["cycleTs"] = cycle_ts

        meter_values = {}

        for dev in cfg.get("devices", []):
            if not dev.get("enabled", True):
                continue

            meter_id = dev["meterId"]
            label = dev.get("label", "")
            ip = dev["ip"]
            port = int(dev.get("port", 502))

            client = ModbusTcpClient(ip, port=port, timeout=1.0)

            if not client.connect():
                latest["devices"][meter_id] = {
                    "ok": False,
                    "data_valid": False,
                    "error": "Modbus connect failed",
                    "ip": ip,
                    "label": label,
                    "cycleTs": cycle_ts,
                }
                meter_values[meter_id] = {"ok": False}
                continue

            try:
                flow_raw = read_u32(client, REG_FLOW_U32)
                total_raw = read_u32(client, REG_TOTAL_U32)
                temp_raw = read_i16(client, REG_TEMP_I16)
                temp_c = temp_raw / 10.0
                stab = read_u16(client, REG_STAB_U16)

                module_status = read_u16(client, REG_MODULE_STATUS)
                data_status = read_u16(client, REG_DATA_STATUS)
                diag_info = read_u16(client, REG_DIAG_INFO)
                event0_code = read_u16(client, REG_EVENT0_CODE)

                # totalizer delta for REAL meter only
                total_delta = calc_total_delta(meter_id, total_raw)

                with RECORDING_LOCK:
                    rec = recording_state.get(meter_id, {
                        "is_on": False,
                        "session_id": None,
                        "started_utc": None
                    })

                # Enqueue raw sample (commit later in batch)
                enqueue_raw_sample({
                    "cycle_ts_utc": cycle_ts,
                    "meter_id": meter_id,
                    "flow_raw": flow_raw,
                    "total_raw": total_raw,
                    "temp_raw": temp_raw,
                    "temp_c": temp_c,
                    "stability": stab,
                    "module_status": module_status,
                    "data_status": data_status,
                    "diag_info": diag_info,
                    "event0_code": event0_code,
                    "data_valid": True
                })

                payload = {
                    "meterId": meter_id,
                    "label": label,
                    "ip": ip,

                    "cycleTs": cycle_ts,
                    "tsUtc": cycle_ts,  # backward compatible

                    "flow_raw": flow_raw,
                    "total_raw": total_raw,
                    "total_delta": total_delta,
                    "temp_raw": temp_raw,
                    "temp_c": temp_c,
                    "stability": stab,

                    "module_status": module_status,
                    "data_status": data_status,
                    "diag_info": diag_info,
                    "event0_code": event0_code,

                    "data_valid": True,

                    "recording": rec["is_on"],
                    "recordingSessionId": rec["session_id"],
                    "recordingSince": rec["started_utc"],

                    "source": "mp-fen1"
                }

                push_meter_latest(tenant_id, site_id, meter_id, payload)

                latest["devices"][meter_id] = {**payload, "ok": True}

                meter_values[meter_id] = {
                    "ok": True,
                    "flow_raw": flow_raw,
                    "total_raw": total_raw,
                    "delta": total_delta
                }

                # ====================================================
                # MIRROR OUTLET: write a simulated outlet from inlet
                # ====================================================
                if mirror_enabled and meter_id == mirror_from:
                    # recording state for simulated outlet (separate switch)
                    with RECORDING_LOCK:
                        sim_rec = recording_state.get(sim_out_meter_id, {
                            "is_on": False,
                            "session_id": None,
                            "started_utc": None
                        })

                    sim_payload = make_mirrored_device(payload, sim_out_cfg)

                    # Overwrite recording fields for simulated meter (so Flutter can control separately)
                    sim_payload["recording"] = sim_rec["is_on"]
                    sim_payload["recordingSessionId"] = sim_rec["session_id"]
                    sim_payload["recordingSince"] = sim_rec["started_utc"]

                    # NOTE: We do NOT recalc total_delta for simulated meter. We reuse inlet values.
                    push_meter_latest(tenant_id, site_id, sim_out_meter_id, sim_payload)

                    latest["devices"][sim_out_meter_id] = {**sim_payload, "ok": True}

                    meter_values[sim_out_meter_id] = {
                        "ok": True,
                        "flow_raw": sim_payload.get("flow_raw"),
                        "total_raw": sim_payload.get("total_raw"),
                        "delta": sim_payload.get("total_delta")
                    }

                    # Optional but useful: store raw sample row for simulated outlet too
                    enqueue_raw_sample({
                        "cycle_ts_utc": cycle_ts,
                        "meter_id": sim_out_meter_id,
                        "flow_raw": sim_payload.get("flow_raw"),
                        "total_raw": sim_payload.get("total_raw"),
                        "temp_raw": sim_payload.get("temp_raw"),
                        "temp_c": sim_payload.get("temp_c"),
                        "stability": sim_payload.get("stability"),
                        "module_status": sim_payload.get("module_status"),
                        "data_status": sim_payload.get("data_status"),
                        "diag_info": sim_payload.get("diag_info"),
                        "event0_code": sim_payload.get("event0_code"),
                        "data_valid": True
                    })

            except Exception as e:
                latest["devices"][meter_id] = {
                    "ok": False,
                    "data_valid": False,
                    "error": str(e),
                    "ip": ip,
                    "label": label,
                    "cycleTs": cycle_ts,
                }
                meter_values[meter_id] = {"ok": False}

            finally:
                client.close()

        # Flush raw samples batch to DB
        flush_db_if_needed(db_conn, cfg)

        # Apply retention about once per minute (cheap)
        now_mono = time.monotonic()
        if now_mono - last_retention_mono >= 60.0:
            apply_raw_retention(db_conn, cfg, cycle_ts)
            last_retention_mono = now_mono

        # Summary/current: synced inlet/outlet + delta-based loss
        inlet = meter_values.get("inlet")
        outlet = meter_values.get("outlet")

        inlet_ok = bool(inlet and inlet.get("ok"))
        outlet_ok = bool(outlet and outlet.get("ok"))

        summary = {
            "cycleTs": cycle_ts,
            "quality": {
                "inlet_ok": inlet_ok,
                "outlet_ok": outlet_ok,
                "sync": inlet_ok and outlet_ok
            },
            "inlet": {"ok": False},
            "outlet": {"ok": False},
            "loss": {"delta": None, "percent": None, "basis": "delta"}
        }

        if inlet_ok:
            summary["inlet"] = {
                "flow_raw": inlet.get("flow_raw"),
                "total_raw": inlet.get("total_raw"),
                "delta": inlet.get("delta"),
                "ok": True
            }

        if outlet_ok:
            summary["outlet"] = {
                "flow_raw": outlet.get("flow_raw"),
                "total_raw": outlet.get("total_raw"),
                "delta": outlet.get("delta"),
                "ok": True
            }

        if inlet_ok and outlet_ok:
            inlet_delta = inlet.get("delta")
            outlet_delta = outlet.get("delta")

            if inlet_delta is not None and outlet_delta is not None and inlet_delta > 0:
                loss_delta = inlet_delta - outlet_delta
                summary["loss"] = {
                    "delta": loss_delta,
                    "percent": (loss_delta / inlet_delta) * 100.0,
                    "basis": "delta"
                }
            else:
                # fallback first cycle
                flow_in = inlet.get("flow_raw") or 0
                flow_out = outlet.get("flow_raw") or 0
                summary["loss"] = {
                    "delta": flow_in - flow_out,
                    "percent": ((flow_in - flow_out) / flow_in * 100.0) if flow_in else None,
                    "basis": "flow_fallback"
                }

        latest["summary"] = summary
        push_summary(tenant_id, site_id, summary)

        time.sleep(poll_s)


# ============================================================
# Flask
# ============================================================
app = Flask(__name__, template_folder="web/templates")


@app.route("/")
def dashboard():
    return render_template("dashboard.html", state=latest, cfg=get_config_live())


@app.route("/settings", methods=["GET", "POST"])
def settings():
    cfg = get_config_live()
    if request.method == "POST":
        devices = []
        meter_ids = request.form.getlist("meterId")
        labels = request.form.getlist("label")
        ips = request.form.getlist("ip")
        ports = request.form.getlist("port")
        enableds = set(map(int, request.form.getlist("enabled")))

        for i in range(len(ips)):
            if not meter_ids[i] or not ips[i]:
                continue
            devices.append({
                "meterId": meter_ids[i].strip(),
                "label": (labels[i] or "").strip(),
                "ip": ips[i].strip(),
                "port": int(ports[i] or 502),
                "enabled": i in enableds
            })

        new_cfg = {
            "tenantId": cfg["tenantId"],  # locked here for now
            "siteId": request.form.get("siteId", cfg["siteId"]).strip(),
            "pollSeconds": float(request.form.get("pollSeconds", cfg.get("pollSeconds", 1))),
            "firestore": cfg.get("firestore", {}),
            "retention": cfg.get("retention", {}),
            "sqlite": cfg.get("sqlite", {}),
            "devices": devices
        }

        save_config(new_cfg)
        set_config_live(new_cfg)

        return redirect(url_for("settings"))

    return render_template("settings.html", cfg=cfg)


# ============================================================
# Main
# ============================================================
def main():
    global fs

    cfg = load_config()
    set_config_live(cfg)

    # init db with config pragmas
    db_conn = init_db(cfg)

    # init firestore
    try:
        fs = init_firestore(cfg)
        latest["firestoreOk"] = True
        latest["lastFirestoreError"] = None
    except Exception as e:
        fs = None
        latest["firestoreOk"] = False
        latest["lastFirestoreError"] = str(e)

    # init recording + delta state
# init states for real devices
for d in cfg.get("devices", []):
    meter_id = d["meterId"]
    recording_state[meter_id] = {"is_on": False, "session_id": None, "started_utc": None}
    with TOTALS_LOCK:
        if meter_id not in last_totals:
            last_totals[meter_id] = None

# init states for simulated outlet (if enabled)
sim = cfg.get("simulate") or {}
mirror_from = sim.get("mirrorOutletFrom")
out_cfg = (sim.get("outlet") or {})
out_meter_id = out_cfg.get("meterId", "outlet")

if mirror_from and out_cfg.get("enabled", True):
    if out_meter_id not in recording_state:
        recording_state[out_meter_id] = {"is_on": False, "session_id": None, "started_utc": None}
    with TOTALS_LOCK:
        if out_meter_id not in last_totals:
            last_totals[out_meter_id] = None


    # start poll + commands
    threading.Thread(target=poll_loop, args=(db_conn,), daemon=True).start()
    threading.Thread(target=process_commands, args=(fs, db_conn, recording_state, get_config_live), daemon=True).start()

    app.run(host="0.0.0.0", port=8080, debug=False)


if __name__ == "__main__":
    main()
