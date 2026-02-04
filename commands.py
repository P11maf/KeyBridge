import time
from datetime import datetime
from recorder import start_recording, stop_recording

COMMAND_POLL_SECONDS = 2.0


def utc_now():
    return datetime.utcnow().isoformat() + "Z"


def process_commands(
    firestore_db,
    db_conn,
    recording_state,
    get_config_live
):
    """
    Polls:
      tenants/{tenantId}/flow_sites/{siteId}/commands
    Executes:
      START_RECORDING / STOP_RECORDING

    Uses get_config_live() so changing tenant/site in config applies without restart.
    """
    if firestore_db is None:
        # no firestore, nothing to do
        while True:
            time.sleep(COMMAND_POLL_SECONDS)

    while True:
        cfg = get_config_live()
        tenant_id = cfg["tenantId"]
        site_id = cfg["siteId"]

        commands_ref = (
            firestore_db
            .collection("tenants").document(tenant_id)
            .collection("flow_sites").document(site_id)
            .collection("commands")
        )

        pending = (
            commands_ref
            .where("status", "==", "pending")
            .limit(5)
            .stream()
        )

        for cmd in pending:
            cmd_ref = commands_ref.document(cmd.id)
            data = cmd.to_dict() or {}

            try:
                # Claim command (best-effort)
                cmd_ref.update({
                    "status": "running",
                    "tsStarted": utc_now()
                })

                cmd_type = data.get("type")
                meter_id = data.get("meterId")
                requested_by = data.get("requestedBy", "unknown")

                if cmd_type == "START_RECORDING":
                    session_id = start_recording(
                        db_conn,
                        recording_state,
                        meter_id,
                        started_by=requested_by
                    )
                    result = {"sessionId": session_id}

                elif cmd_type == "STOP_RECORDING":
                    session_id = stop_recording(
                        db_conn,
                        recording_state,
                        meter_id,
                        ended_by=requested_by
                    )
                    result = {"sessionId": session_id}

                else:
                    raise ValueError(f"Unknown command type {cmd_type}")

                cmd_ref.update({
                    "status": "done",
                    "tsCompleted": utc_now(),
                    "result": result
                })

            except Exception as e:
                cmd_ref.update({
                    "status": "error",
                    "error": str(e),
                    "tsCompleted": utc_now()
                })

        time.sleep(COMMAND_POLL_SECONDS)
