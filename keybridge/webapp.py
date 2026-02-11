from flask import Flask, render_template, request, redirect, url_for, Response
import json
import uuid

from keybridge.paths import TEMPLATES_DIR
from keybridge.config_store import save_config, set_config_live


def _to_bool(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _safe_gateway_id(current: str | None) -> str:
    cur = (current or "").strip()
    if cur:
        return cur
    return f"gateway-{uuid.uuid4().hex[:6]}"


def create_app(latest: dict, get_config_live):
    app = Flask(__name__, template_folder=TEMPLATES_DIR)

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html", state=latest, cfg=get_config_live())

    @app.route("/settings", methods=["GET", "POST"])
    def settings():
        cfg = get_config_live()

        if request.method == "POST":
            # --- Identity / Mode ---
            tenant_id = (request.form.get("tenantId") or cfg.get("tenantId") or "").strip()
            site_id = (request.form.get("siteId") or cfg.get("siteId") or "").strip()
            gateway_id = _safe_gateway_id(request.form.get("gatewayId") or cfg.get("gatewayId"))
            mode = (request.form.get("mode") or cfg.get("mode") or "gateway").strip().lower()

            try:
                poll_seconds = float(request.form.get("pollSeconds") or cfg.get("pollSeconds") or 1.0)
            except Exception:
                poll_seconds = float(cfg.get("pollSeconds") or 1.0)

            # --- Devices table (array fields) ---
            devices = []
            meter_ids = request.form.getlist("meterId")
            labels = request.form.getlist("label")
            ips = request.form.getlist("ip")
            ports = request.form.getlist("port")
            unit_ids = request.form.getlist("unitId")
            profiles = request.form.getlist("profile")
            roles = request.form.getlist("role")
            sources = request.form.getlist("source")

            enabled_meter_ids = set((request.form.getlist("enabled_meterId") or []))

            n = max(
                len(meter_ids), len(labels), len(ips), len(ports),
                len(unit_ids), len(profiles), len(roles), len(sources)
            )

            def get_at(lst, i, default=""):
                return lst[i] if i < len(lst) else default

            for i in range(n):
                mid = (get_at(meter_ids, i, "") or "").strip()
                if not mid:
                    continue

                source = (get_at(sources, i, "mp-fen1") or "mp-fen1").strip()
                ip = (get_at(ips, i, "") or "").strip()

                # ✅ Only require IP for real modbus sources
                if source != "simulated" and not ip:
                    continue

                label = (get_at(labels, i, "") or "").strip()

                try:
                    port = int(get_at(ports, i, "502") or 502)
                except Exception:
                    port = 502

                try:
                    unit_id = int(get_at(unit_ids, i, "1") or 1)
                except Exception:
                    unit_id = 1

                profile = (get_at(profiles, i, "fd-r-v1") or "fd-r-v1").strip()
                role = (get_at(roles, i, "") or "").strip()

                devices.append({
                    "meterId": mid,
                    "label": label,
                    "ip": ip if ip else None,        # allow None for simulated
                    "port": port,
                    "unitId": unit_id,
                    "profile": profile,
                    "role": role,
                    "source": source,
                    "enabled": (mid in enabled_meter_ids),
                })

            # --- Simulation settings (global) ---
            simulate_enabled = _to_bool(request.form.get("simulate_enabled", "false"))
            simulate_meter_id = (request.form.get("simulate_meterId") or "sim_meter").strip()
            simulate_label = (request.form.get("simulate_label") or "Simulated Meter").strip()

            simulate_cfg = dict(cfg.get("simulate", {}) or {})
            simulate_cfg["enabled"] = simulate_enabled
            simulate_cfg["meterId"] = simulate_meter_id
            simulate_cfg["label"] = simulate_label

            # --- Summary rules (Aggregator) ---
            summary_enabled = _to_bool(request.form.get("summary_enabled", "false"))
            summary_rules = {
                "enabled": summary_enabled,
                "inletMeterId": (request.form.get("summary_inletMeterId") or "").strip(),
                "outletMeterId": (request.form.get("summary_outletMeterId") or "").strip(),
                "writePath": (request.form.get("summary_writePath") or "summary/current").strip(),
                "maxAgeSeconds": int(request.form.get("summary_maxAgeSeconds") or 10),
                "requireSync": _to_bool(request.form.get("summary_requireSync", "false")),
            }

            new_cfg = {
                "version": 2,
                "tenantId": tenant_id,
                "siteId": site_id,
                "gatewayId": gateway_id,
                "mode": mode,
                "pollSeconds": poll_seconds,
                "firestore": cfg.get("firestore", {}) or {},
                "retention": cfg.get("retention", {}) or {},
                "sqlite": cfg.get("sqlite", {}) or {},
                "simulate": simulate_cfg,
                "devices": devices,
                "summaryRules": summary_rules,
            }

            save_config(new_cfg)
            set_config_live(new_cfg)
            return redirect(url_for("settings"))

        return render_template("settings.html", cfg=cfg, latest=latest)

    @app.route("/debug")
    def debug():
        return Response(json.dumps(latest, indent=2, default=str), mimetype="application/json")

    return app
