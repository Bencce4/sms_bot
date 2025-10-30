import os

APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
UPLOADS_DIR = os.path.join(DATA_DIR, "uploads")
CACHE_DIR = os.path.join(DATA_DIR, "cache")

os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
