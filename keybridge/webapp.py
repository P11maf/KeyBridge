from flask import Flask, render_template, request, redirect, url_for, Response
import json

from keybridge.paths import TEMPLATES_DIR
from keybridge.config_store import save_config, set_config_live


def create_app(latest: dict, get_config_live):
    app = Flask(__name__, template_folder=TEMPLATES_DIR)

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
                "tenantId": cfg["tenantId"],
                "siteId": request.form.get("siteId", cfg["siteId"]).strip(),
                "pollSeconds": float(request.form.get("pollSeconds", cfg.get("pollSeconds", 1))),
                "firestore": cfg.get("firestore", {}),
                "retention": cfg.get("retention", {}),
                "sqlite": cfg.get("sqlite", {}),
                "simulate": cfg.get("simulate", {}),
                "devices": devices
            }

            save_config(new_cfg)
            set_config_live(new_cfg)
            return redirect(url_for("settings"))

        return render_template("settings.html", cfg=cfg, latest=latest)

    # ✅ NEW: raw JSON view of runtime state
    @app.route("/debug")
    def debug():
        return Response(json.dumps(latest, indent=2, default=str), mimetype="application/json")

    return app
