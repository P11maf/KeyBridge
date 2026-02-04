import json
import threading
from keybridge.paths import CONFIG_PATH

CONFIG_LOCK = threading.Lock()
CONFIG = None

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(cfg: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

def set_config_live(cfg: dict):
    global CONFIG
    with CONFIG_LOCK:
        CONFIG = cfg

def get_config_live() -> dict:
    with CONFIG_LOCK:
        return json.loads(json.dumps(CONFIG))
