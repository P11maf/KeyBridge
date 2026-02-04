import threading

from keybridge.config_store import load_config, set_config_live, get_config_live
from keybridge.db import init_db
from keybridge.firestore_client import init_firestore
from keybridge.state import latest, recording_state, last_totals, init_runtime_state
from keybridge.polling import poll_loop
from keybridge.webapp import create_app

from commands import process_commands  # keep your existing module
# from recorder import start_recording, stop_recording  # used by commands.py


def main():
    cfg = load_config()
    set_config_live(cfg)

    db_conn = init_db(cfg)

    fs = init_firestore(cfg, latest)

    # initialize recording_state + last_totals including simulation outlet
    init_runtime_state(cfg, recording_state, last_totals)

    # start poll + command worker
    threading.Thread(target=poll_loop, args=(db_conn, fs, latest, recording_state, last_totals, get_config_live), daemon=True).start()
    threading.Thread(target=process_commands, args=(fs, db_conn, recording_state, get_config_live), daemon=True).start()

    app = create_app(latest, get_config_live)
    app.run(host="0.0.0.0", port=8080, debug=False)


if __name__ == "__main__":
    main()
