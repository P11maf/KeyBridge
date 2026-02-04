def maybe_simulate_outlet(cfg: dict, cycle_ts: str, inlet_doc: dict) -> dict:
    """
    Returns an outlet_doc. If simulation is enabled (mirrorOutletFrom=inlet),
    mirror inlet values into outlet and mark source='simulated'.
    If simulation is disabled, return a minimal 'not ok' outlet doc.

    This function exists as a stable API for polling.py.
    """
    sim = cfg.get("simulate", {}) or {}

    # Default outlet shape (disabled / not available)
    outlet = {
        "cycleTs": cycle_ts,
        "meterId": "outlet",
        "label": "Outlet",
        "ok": False,
        "source": "none",
        "error": "Outlet not configured",
        "flow_raw": None,
        "total_raw": None,
        "temp_raw": None,
        "temp_c": None,
        "stability": None,
        "module_status": None,
        "data_status": None,
        "diag_info": None,
        "event0_code": None,
        "data_valid": False,
    }

    outlet_cfg = (sim.get("outlet", {}) or {})
    enabled = bool(outlet_cfg.get("enabled", False))
    mirror_from = sim.get("mirrorOutletFrom")

    if not enabled:
        return outlet

    outlet["ok"] = True
    outlet["source"] = "simulated"
    outlet["label"] = outlet_cfg.get("label", "Outlet (Simulated)")
    outlet["meterId"] = outlet_cfg.get("meterId", "outlet")

    # Mirror from inlet (only supported mode right now)
    if mirror_from == "inlet":
        # Copy core readings
        for k in [
            "flow_raw", "total_raw", "temp_raw", "temp_c", "stability",
            "module_status", "data_status", "diag_info", "event0_code", "data_valid"
        ]:
            outlet[k] = inlet_doc.get(k)

        # If inlet isn't ok, reflect that
        outlet["ok"] = bool(inlet_doc.get("ok", True))
        if not outlet["ok"]:
            outlet["error"] = inlet_doc.get("error", "Inlet not OK; simulated outlet mirrored error")
    else:
        outlet["ok"] = False
        outlet["error"] = "Simulation enabled but mirrorOutletFrom not set to 'inlet'"

    return outlet
