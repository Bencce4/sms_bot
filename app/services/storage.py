import os, time, json
import pandas as pd
from app.settings import UPLOADS_DIR, CACHE_DIR

def save_upload(file_bytes: bytes, filename: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    safe = filename.replace("/", "_")
    path = os.path.join(UPLOADS_DIR, f"{ts}__{safe}")
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path

def latest_excel_path() -> str | None:
    files = [os.path.join(UPLOADS_DIR, f) for f in os.listdir(UPLOADS_DIR) if f.lower().endswith((".xlsx",".xls"))]
    if not files:
        return None
    return sorted(files)[-1]

def load_sheets(xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    people = pd.read_excel(xlsx_path, sheet_name="Kandidatas")
    projects = pd.read_excel(xlsx_path, sheet_name="Projektai")
    return people, projects

def save_matches_df(df: pd.DataFrame) -> str:
    path = os.path.join(CACHE_DIR, "matches.json")
    df.to_json(path, orient="records", force_ascii=False)
    return path

def load_matches() -> list[dict]:
    path = os.path.join(CACHE_DIR, "matches.json")
    if not os.path.exists(path):
        return []
    return json.load(open(path, "r", encoding="utf-8"))
