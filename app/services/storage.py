# app/services/storage.py
from pathlib import Path
import json
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
CACHE_DIR = DATA_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

CACHE_MATCHES = CACHE_DIR / "matches.json"

# Map many possible header variants -> canonical keys
COLMAP = {
    # city
    "miestas": "Miestas",
    "city": "Miestas",
    "iš kokio miesto": "Miestas",   # <= NEW

    # profession
    "specialybė": "Specialybė",
    "specialybe": "Specialybė",
    "profession": "Specialybė",
    "prof": "Specialybė",
    "kvalifikacija": "Specialybė",  # <= NEW

    # phone
    "tel. nr": "Tel. nr",
    "tel nr": "Tel. nr",
    "phone": "Tel. nr",
    "telefonas": "Tel. nr",

    # text
    "sms_text": "sms_text",
    "sms text": "sms_text",
    "text": "sms_text",
    "message": "sms_text",

    # optional id
    "match_id": "match_id",
    "id": "match_id",
}

CANON_ORDER = ["match_id", "Miestas", "Specialybė", "Tel. nr", "sms_text"]

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    # lower/strip, dedupe, then map to canonical
    new_cols = []
    seen = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if k in seen:
            # make unique
            seen[k] += 1
            k = f"{k}__{seen[k]}"
        else:
            seen[k] = 0
        new_cols.append(k)
    df = df.copy()
    df.columns = new_cols

    # map to canonical names where possible
    mapped = []
    for c in df.columns:
        mapped.append(COLMAP.get(c, COLMAP.get(c.replace("_", " "), COLMAP.get(c.replace(".", "").strip(), c))))
    df.columns = mapped
    return df

def save_upload(raw_bytes: bytes, filename: str) -> Path:
    p = UPLOAD_DIR / f"{pd.Timestamp.now():%Y%m%d_%H%M%S}__{filename}"
    p.write_bytes(raw_bytes)
    return p

def latest_excel_path() -> Path | None:
    xs = sorted(UPLOAD_DIR.glob("*.xlsx"))
    return xs[-1] if xs else None

def load_sheets(path: Path) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """
    Tries hard to read your Excel.
    - If two sheets exist, treat sheet0 as 'people', sheet1 as 'projects'
    - If one sheet exists and already has phone+text, treat it as ready-to-send
    """
    xl = pd.ExcelFile(path)
    sheet_names = xl.sheet_names

    # read all sheets and normalize
    dfs = [ _normalize_headers(pd.read_excel(xl, s)) for s in sheet_names ]

    if len(dfs) >= 2:
        people = dfs[0]
        projects = dfs[1]
    else:
        people = dfs[0]
        projects = None

    return people, projects

def save_matches_df(df: pd.DataFrame) -> None:
    # keep only canonical columns we know how to render
    if "match_id" not in df.columns:
        df = df.copy()
        df["match_id"] = range(1, len(df) + 1)
    for col in CANON_ORDER:
        if col not in df.columns:
            df[col] = ""
    df = df[CANON_ORDER].fillna("")
    df.to_json(CACHE_MATCHES, orient="records", force_ascii=False)

def load_matches() -> list[dict]:
    if not CACHE_MATCHES.exists():
        return []
    return json.loads(CACHE_MATCHES.read_text(encoding="utf-8"))
