import json
import threading
import uuid

from keybridge.paths import CONFIG_PATH

CONFIG_LOCK = threading.Lock()
CONFIG = None


def _to_bool(v, default=False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def normalize_config(cfg: dict) -> dict:
    """
    Ensure config is vNext-safe even if an older config.json is used.

    Adds defaults for:
      - version
      - gatewayId
      - mode
      - summaryRules
      - devices fields (unitId/profile/source/role/enabled)
    """
    if not isinstance(cfg, dict):
        cfg = {}

    out = dict(cfg)

    # version
    out["version"] = int(out.get("version") or 2)

    # identity
    out.setdefault("tenantId", "leprino")
    out.setdefault("siteId", "default_site")

    # gatewayId
    gid = (out.get("gatewayId") or "").strip()
    if not gid:
        gid = f"gateway-{uuid.uuid4().hex[:6]}"
    out["gatewayId"] = gid

    # mode
    mode = (out.get("mode") or "").strip().lower()
    if mode not in ("gateway", "aggregator", "both"):
        # Back-compat: if simulate outlet enabled, default to both so summary can keep working
        sim = out.get("simulate", {}) or {}
        sim_out = (sim.get("outlet", {}) or {})
        if _to_bool(sim_out.get("enabled"), False):
            mode = "both"
        else:
            mode = "gateway"
    out["mode"] = mode

    # firestore/retention/sqlite/simulate defaults (keep existing if present)
    out.setdefault("firestore", {})
    out.setdefault("retention", {})
    out.setdefault("sqlite", {})
    out.setdefault("simulate", {})

    # devices
    devices = out.get("devices", []) or []
    if not isinstance(devices, list):
        devices = []
    norm_devices = []
    for d in devices:
        if not isinstance(d, dict):
            continue
        nd = dict(d)
        nd["meterId"] = (nd.get("meterId") or "").strip()
        if not nd["meterId"]:
            continue
        nd["label"] = (nd.get("label") or "").strip()
        nd["ip"] = (nd.get("ip") or "").strip()
        # port default
        try:
            nd["port"] = int(nd.get("port") or 502)
        except Exception:
            nd["port"] = 502
        # vNext fields defaults
        nd["enabled"] = _to_bool(nd.get("enabled"), True)
        try:
            nd["unitId"] = int(nd.get("unitId") or 1)
        except Exception:
            nd["unitId"] = 1
        nd["profile"] = (nd.get("profile") or "fd-r-v1").strip()
        nd["role"] = (nd.get("role") or "").strip()
        nd["source"] = (nd.get("source") or "mp-fen1").strip()
        norm_devices.append(nd)

    out["devices"] = norm_devices

    # summaryRules
    sr = out.get("summaryRules")
    if not isinstance(sr, dict):
        sr = {}

    sim = out.get("simulate", {}) or {}
    sim_out = (sim.get("outlet", {}) or {})
    sim_enabled = _to_bool(sim_out.get("enabled"), False)

    # If simulate outlet enabled, default summary enabled if not set
    sr_enabled = sr.get("enabled")
    if sr_enabled is None:
        sr_enabled = sim_enabled
    sr["enabled"] = _to_bool(sr_enabled, False)

    sr.setdefault("inletMeterId", "inlet")
    sr.setdefault("outletMeterId", sim_out.get("meterId") or "outlet")
    sr.setdefault("writePath", "summary/current")
    try:
        sr["maxAgeSeconds"] = int(sr.get("maxAgeSeconds") or 10)
    except Exception:
        sr["maxAgeSeconds"] = 10
    sr["requireSync"] = _to_bool(sr.get("requireSync"), False)

    out["summaryRules"] = sr

    # pollSeconds
    try:
        out["pollSeconds"] = float(out.get("pollSeconds") or 1.0)
    except Exception:
        out["pollSeconds"] = 1.0

    return out


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return normalize_config(cfg)


def save_config(cfg: dict):
    cfg = normalize_config(cfg)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def set_config_live(cfg: dict):
    global CONFIG
    with CONFIG_LOCK:
        CONFIG = normalize_config(cfg)


def get_config_live() -> dict:
    with CONFIG_LOCK:
        # deep copy
        return json.loads(json.dumps(CONFIG))
