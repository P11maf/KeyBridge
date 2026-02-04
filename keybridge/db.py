import sqlite3
import os
import time
import threading
from datetime import datetime, timezone, timedelta

from keybridge.paths import DATA_DIR, DB_PATH

DB_LOCK = threading.Lock()
pending_raw_rows = []
last_commit_monotonic = time.monotonic()

def init_db(cfg: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    # pragmas
    try:
        pragmas = (cfg.get("sqlite", {}) or {}).get("pragmas", {}) or {}
        journal = pragmas.get("journal_mode")
        sync = pragmas.get("synchronous")
        if journal:
            conn.execute(f"PRAGMA journal_mode={journal}")
        if sync:
            conn.execute(f"PRAGMA synchronous={sync}")
    except Exception:
        pass

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

def enqueue_raw_sample(row: dict):
    global pending_raw_rows
    with DB_LOCK:
        pending_raw_rows.append(row)

def flush_db_if_needed(db_conn, cfg: dict, latest: dict):
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

def apply_raw_retention(db_conn, cfg: dict, cycle_ts_iso: str, latest: dict):
    retention = cfg.get("retention", {}) or {}
    if not retention.get("raw_seconds_enabled", True):
        return

    window_min = int(retention.get("raw_seconds_window_minutes", 60))
    cutoff = datetime.fromisoformat(cycle_ts_iso.replace("Z", "+00:00")) - timedelta(minutes=window_min)
    cutoff_iso = cutoff.astimezone(timezone.utc).isoformat()

    try:
        with db_conn:
            db_conn.execute("DELETE FROM raw_samples WHERE cycle_ts_utc < ?", (cutoff_iso,))
    except Exception as e:
        latest["localDbOk"] = False
        latest["lastLocalDbError"] = str(e)
