# app/services/storage.py
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timezone
from uuid import uuid4
from typing import Dict, List  # noqa: F401 (Dict may be unused in some modules)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
CACHE_DIR = DATA_DIR / "cache"
CHATS_DIR = DATA_DIR / "chats"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CHATS_DIR.mkdir(parents=True, exist_ok=True)

CACHE_MATCHES = CACHE_DIR / "matches.json"

# --------- Excel header normalization ----------
COLMAP = {
    # city
    "miestas": "Miestas",
    "city": "Miestas",
    "iš kokio miesto": "Miestas",

    # profession
    "specialybė": "Specialybė",
    "specialybe": "Specialybė",
    "profession": "Specialybė",
    "prof": "Specialybė",
    "kvalifikacija": "Specialybė",

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
    new_cols = []
    seen = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if k in seen:
            seen[k] += 1
            k = f"{k}__{seen[k]}"
        else:
            seen[k] = 0
        new_cols.append(k)
    df = df.copy()
    df.columns = new_cols

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
    Read Excel robustly.
    If two sheets exist, treat [0] as 'people' and [1] as 'projects'.
    If one, return it as 'people' and projects=None.
    """
    xl = pd.ExcelFile(path)
    sheet_names = xl.sheet_names

    dfs = [_normalize_headers(pd.read_excel(xl, s)) for s in sheet_names]

    if len(dfs) >= 2:
        people = dfs[0]
        projects = dfs[1]
    else:
        people = dfs[0]
        projects = None

    return people, projects

def save_matches_df(df: pd.DataFrame) -> None:
    df = df.copy()
    if "match_id" not in df.columns:
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

# --------- Optional JSONL chat helpers (not used by DB chat, but safe to have) ----------
def _chat_path(conv_id: str) -> Path:
    return CHATS_DIR / f"{conv_id}.jsonl"

def chat_load(conv_id: str) -> list[dict]:
    p = _chat_path(conv_id)
    if not p.exists():
        return []
    rows = []
    for line in p.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows

def chat_append(conv_id: str, role: str, content: str) -> None:
    p = _chat_path(conv_id)
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "role": role,
        "content": content,
    }
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def new_conv_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"conv-{ts}-{uuid4().hex[:6]}"

def chat_clear(conv_id: str) -> None:
    p = _chat_path(conv_id)
    if p.exists():
        p.unlink()
