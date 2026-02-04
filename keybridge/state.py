import threading

# Locks shared across threads
RECORDING_LOCK = threading.Lock()
TOTALS_LOCK = threading.Lock()

# Runtime state
recording_state = {}   # meterId -> {is_on, session_id, started_utc}
last_totals = {}       # meterId -> last_total_raw

# Dashboard state (shared)
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

def init_runtime_state(cfg: dict, recording_state: dict, last_totals: dict):
    # real devices
    for d in cfg.get("devices", []):
        meter_id = d["meterId"]
        if meter_id not in recording_state:
            recording_state[meter_id] = {"is_on": False, "session_id": None, "started_utc": None}
        if meter_id not in last_totals:
            last_totals[meter_id] = None

    # simulated outlet
    sim = cfg.get("simulate") or {}
    mirror_from = sim.get("mirrorOutletFrom")
    out_cfg = sim.get("outlet") or {}
    out_meter_id = out_cfg.get("meterId", "outlet")

    if mirror_from and out_cfg.get("enabled", True):
        if out_meter_id not in recording_state:
            recording_state[out_meter_id] = {"is_on": False, "session_id": None, "started_utc": None}
        if out_meter_id not in last_totals:
            last_totals[out_meter_id] = None
