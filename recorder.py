from datetime import datetime
import threading

RECORDING_LOCK = threading.Lock()


def utc_now():
    return datetime.utcnow().isoformat() + "Z"


def start_recording(db_conn, recording_state, meter_id, started_by):
    with RECORDING_LOCK:
        state = recording_state.get(meter_id)
        if not state:
            raise ValueError("Unknown meter")

        if state.get("is_on"):
            raise RuntimeError("Recording already active")

        session_id = f"rec_{meter_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        started_utc = utc_now()

        db_conn.execute(
            """
            INSERT INTO recording_sessions
            (session_id, meter_id, started_utc, started_by)
            VALUES (?, ?, ?, ?)
            """,
            (session_id, meter_id, started_utc, started_by)
        )
        db_conn.commit()

        state["is_on"] = True
        state["session_id"] = session_id
        state["started_utc"] = started_utc

        return session_id


def stop_recording(db_conn, recording_state, meter_id, ended_by):
    with RECORDING_LOCK:
        state = recording_state.get(meter_id)
        if not state or not state.get("is_on"):
            raise RuntimeError("Recording not active")

        ended_utc = utc_now()
        session_id = state.get("session_id")

        db_conn.execute(
            """
            UPDATE recording_sessions
            SET ended_utc = ?, ended_by = ?
            WHERE session_id = ?
            """,
            (ended_utc, ended_by, session_id)
        )
        db_conn.commit()

        state["is_on"] = False
        state["session_id"] = None
        state["started_utc"] = None

        return session_id
