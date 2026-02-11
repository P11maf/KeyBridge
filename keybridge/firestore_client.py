import os
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials as fb_credentials

from google.cloud import firestore as gcf
from google.oauth2 import service_account

from keybridge.paths import SERVICE_ACCOUNT_DEFAULT, PROJECT_DIR


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


# ───────────────────────────────────────────────────────────────────────────────
# Path helpers
# ───────────────────────────────────────────────────────────────────────────────

def _flow_site_ref(fs, tenant_id: str, site_id: str):
    return (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
    )


def _meter_doc_ref(fs, tenant_id: str, site_id: str, meter_id: str):
    return (
        _flow_site_ref(fs, tenant_id, site_id)
        .collection("meters")
        .document(meter_id)
    )


# ───────────────────────────────────────────────────────────────────────────────
# NEW: Ensure flow_sites/{siteId} exists so Flutter can list sites.
# ───────────────────────────────────────────────────────────────────────────────

def ensure_flow_site_doc(
    fs,
    tenant_id: str,
    site_id: str,
    gateway_id: str | None = None,
    site_name: str | None = None,
) -> None:
    """
    Creates/updates:
      tenants/{tenantId}/flow_sites/{siteId}

    Firestore allows subcollections to exist without parent docs.
    Flutter lists sites by querying flow_sites collection documents,
    so we MUST ensure the parent doc exists.

    Safe to call every write (merge=True).
    """
    ref = _flow_site_ref(fs, tenant_id, site_id)

    payload = {
        "siteId": site_id,
        "updatedUtc": _utc_now_iso(),
    }
    if site_name:
        payload["name"] = site_name
    if gateway_id:
        payload["lastGatewayId"] = gateway_id

    ref.set(payload, merge=True)


# ───────────────────────────────────────────────────────────────────────────────
# Reads
# ───────────────────────────────────────────────────────────────────────────────

def get_meter_latest(fs, tenant_id: str, site_id: str, meter_id: str) -> dict | None:
    """
    Reads tenants/{tenantId}/flow_sites/{siteId}/meters/{meterId}
    Returns dict or None if missing.
    """
    snap = _meter_doc_ref(fs, tenant_id, site_id, meter_id).get()
    if not snap.exists:
        return None
    return snap.to_dict() or {}


# ───────────────────────────────────────────────────────────────────────────────
# Writes (meter latest)
# ───────────────────────────────────────────────────────────────────────────────

def push_meter_latest(fs, tenant_id, site_id, meter_id: str, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/meters/{meterId}
    Legacy un-guarded write.

    ALSO ensures flow_sites/{siteId} exists (important for Flutter site listing).
    """
    gateway_id = (payload.get("gatewayId") or "").strip() or None
    ensure_flow_site_doc(fs, tenant_id, site_id, gateway_id=gateway_id)

    ref = _meter_doc_ref(fs, tenant_id, site_id, meter_id)

    # Flutter-friendly fields
    p = dict(payload)
    if "name" not in p and "label" in p and (p.get("label") or "").strip():
        p["name"] = p["label"]
    p.setdefault("meterId", meter_id)
    p.setdefault("updatedUtc", p.get("cycleTs") or _utc_now_iso())

    ref.set(p, merge=True)


def push_meter_latest_guarded(
    fs,
    tenant_id: str,
    site_id: str,
    meter_id: str,
    payload: dict,
    gateway_id: str | None,
    latest: dict | None = None,
) -> bool:
    """
    Multi-gateway safety: only write if meter doc is unclaimed OR owned by this gatewayId.

    Rules:
    - If existing doc has gatewayId and it's different: SKIP write and return False.
    - Else: write with merge=True and ensure gatewayId is set.

    Also ensures flow_sites/{siteId} exists for Flutter site listing.
    """
    gid = (gateway_id or "").strip()
    ref = _meter_doc_ref(fs, tenant_id, site_id, meter_id)

    try:
        # Ensure parent site doc exists (CRITICAL for Flutter "sites" dropdown)
        ensure_flow_site_doc(fs, tenant_id, site_id, gateway_id=gid or None)

        snap = ref.get()
        if snap.exists:
            existing = snap.to_dict() or {}
            existing_gid = (existing.get("gatewayId") or "").strip()
            if existing_gid and gid and existing_gid != gid:
                if latest is not None:
                    latest.setdefault("ownershipConflicts", [])
                    latest["ownershipConflicts"].append({
                        "meterId": meter_id,
                        "existingGatewayId": existing_gid,
                        "thisGatewayId": gid,
                        "ts": _utc_now_iso(),
                    })
                return False

        # Build payload (add gatewayId + Flutter-friendly fields)
        p = dict(payload)
        if gid:
            p["gatewayId"] = gid

        if "name" not in p and "label" in p and (p.get("label") or "").strip():
            p["name"] = p["label"]

        p.setdefault("meterId", meter_id)
        p.setdefault("updatedUtc", p.get("cycleTs") or _utc_now_iso())

        ref.set(p, merge=True)
        return True

    except Exception as e:
        if latest is not None:
            latest["lastFirestoreError"] = str(e)
        return False


# ───────────────────────────────────────────────────────────────────────────────
# Summary
# ───────────────────────────────────────────────────────────────────────────────

def push_summary_current(fs, tenant_id, site_id, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/summary/current
    """
    # Ensure parent exists so site appears even if only aggregator writes
    gateway_id = (payload.get("gatewayId") or "").strip() or None
    ensure_flow_site_doc(fs, tenant_id, site_id, gateway_id=gateway_id)

    ref = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("flow_sites")
        .document(site_id)
        .collection("summary")
        .document("current")
    )
    ref.set(payload, merge=True)


# ───────────────────────────────────────────────────────────────────────────────
# Heartbeat
# ───────────────────────────────────────────────────────────────────────────────

def push_gateway_heartbeat(
    fs,
    tenant_id: str,
    gateway_id: str,
    site_id: str | None = None,
    status: str = "ok",
    version: str = "vNext",
    mode: str | None = None,
    extra: dict | None = None,
):
    """
    tenants/{tenantId}/gateways/{gatewayId}
    """
    ref = (
        fs.collection("tenants")
        .document(tenant_id)
        .collection("gateways")
        .document(gateway_id)
    )

    payload = {
        "lastSeen": _utc_now_iso(),
        "status": status,
        "version": version,
    }
    if site_id:
        payload["siteId"] = site_id
    if mode:
        payload["mode"] = mode
    if extra:
        payload.update(extra)

    ref.set(payload, merge=True)


# ───────────────────────────────────────────────────────────────────────────────
# Minute rollups (site-level legacy path)
# ───────────────────────────────────────────────────────────────────────────────

def push_minute_rollup(fs, tenant_id, site_id, minute_start_iso: str, payload: dict):
    """
    tenants/{tenantId}/flow_sites/{siteId}/rollups_minute/{YYYYMMDD_HHMM}
    (Legacy site-level rollup path.)
    """
    ensure_flow_site_doc(fs, tenant_id, site_id)

    doc_id = (
        minute_start_iso.replace("-", "")
        .replace(":", "")
        .replace("T", "_")
        .replace("Z", "")
    )

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
