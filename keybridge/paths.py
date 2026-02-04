import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# BASE_DIR is keybridge/ — we want project root:
PROJECT_DIR = os.path.dirname(BASE_DIR)

CONFIG_PATH = os.path.join(PROJECT_DIR, "config.json")
DATA_DIR = os.path.join(PROJECT_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "keybridge.sqlite")
SERVICE_ACCOUNT_DEFAULT = os.path.join(PROJECT_DIR, "serviceAccountKey.json")

TEMPLATES_DIR = os.path.join(PROJECT_DIR, "web", "templates")
