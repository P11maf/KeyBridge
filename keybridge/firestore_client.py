import os

import firebase_admin
from firebase_admin import credentials as fb_credentials

from google.cloud import firestore as gcf
from google.oauth2 import service_account

from keybridge.paths import SERVICE_ACCOUNT_DEFAULT, PROJECT_DIR


def init_firestore(cfg: dict, latest: dict):
    """
    Initializes Firestore client for a specific databaseId.

    - If firestore.databaseId is missing, defaults to "(default)"
    - Uses serviceAccountPath to authenticate
    - Returns a google-cloud-firestore Client bound to the database
    """
    key_path = SERVICE_ACCOUNT_DEFAULT
    service_path = (cfg.get("firestore", {}) or {}).get("serviceAccountPath")
    if service_path:
        key_path = os.path.join(PROJECT_DIR, service_path)

    db_id = (cfg.get("firestore", {}) or {}).get("databaseId") or "(default)"

    if not os.path.exists(key_path):
        latest["firestoreOk"] = False
        latest["lastFirestoreError"] = f"Missing service account JSON: {key_path}"
        return None

    try:
        creds = service_account.Credentials.from_service_account_file(key_path)

        # Keep firebase_admin init (harmless, and useful if you add admin features later).
        if not firebase_admin._apps:
            firebase_admin.initialize_app(fb_credentials.Certificate(key_path))

        fs = gcf.Client(
            project=creds.project_id,
            credentials=creds,
            database=db_id,
        )

        latest["firestoreOk"] = True
        latest["lastFirestoreError"] = None
        latest["firestoreProjectId"] = creds.project_id
        latest["firestoreDatabaseId"] = db_id
        return fs

    except Exception as e:
        latest["firestoreOk"] = False
        latest["lastFirestoreError"] = str(e)
        return None


def push_meter_latest(fs, tenant_id, site_id, meter_id: str, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/meters/{meterId}
    """
    ref = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
        .collection("meters")
        .document(meter_id)
    )
    ref.set(payload, merge=True)


def push_summary_current(fs, tenant_id, site_id, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/summary/current
    """
    ref = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
        .collection("summary")
        .document("current")
    )
    ref.set(payload, merge=True)


def push_minute_rollup(fs, tenant_id, site_id, minute_start_iso: str, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/rollups_minute/{YYYYMMDD_HHMM}
    """
    doc_id = (
        minute_start_iso.replace("-", "")
        .replace(":", "")
        .replace("T", "_")
        .replace("Z", "")
    )  # 20260130_151200

    if doc_id.endswith("00"):
        doc_id = doc_id[:-2]

    ref = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
        .collection("rollups_minute")
        .document(doc_id)
    )
    ref.set(payload, merge=True)


def delete_old_minute_rollups(fs, tenant_id, site_id, cutoff_iso: str, limit: int = 200) -> int:
    """
    Best-effort retention: delete rollups where minuteStart < cutoff_iso.
    Returns deleted count (up to limit).
    """
    col = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
        .collection("rollups_minute")
    )

    q = col.where("minuteStart", "<", cutoff_iso).order_by("minuteStart").limit(limit)
    docs = list(q.stream())

    deleted = 0
    for d in docs:
        d.reference.delete()
        deleted += 1
    return deleted
