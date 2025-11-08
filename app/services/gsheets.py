# app/services/gsheets.py
import os, time, logging
from typing import List, Sequence
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger("gsheets")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("GSHEETS_SPREADSHEET_ID", "").strip()
CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/srv/app/creds/sa.json")

_service = None

def _build_service():
    if not SPREADSHEET_ID:
        raise RuntimeError("GSHEETS_SPREADSHEET_ID is empty")
    if not CREDS_PATH or not os.path.isfile(CREDS_PATH):
        raise RuntimeError(f"GOOGLE_APPLICATION_CREDENTIALS points to a missing file: {CREDS_PATH}")
    creds = service_account.Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def _svc():
    global _service
    if _service is None:
        _service = _build_service()
        log.info("gsheets: client initialized with %s", CREDS_PATH)
    return _service

def ensure_headers(sheet_title: str, headers: Sequence[str]):
    svc = _svc().spreadsheets()
    # Create sheet if missing
    meta = svc.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = {s["properties"]["title"]: s for s in meta.get("sheets", [])}

    if sheet_title not in sheets:
        log.info("gsheets: creating sheet '%s'", sheet_title)
        svc.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_title}}}]},
        ).execute()
        rng = f"{sheet_title}!A1:{chr(64+len(headers))}1"
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=rng,
            valueInputOption="RAW",
            body={"values": [list(headers)]},
        ).execute()
        log.info("gsheets: headers written to new sheet '%s'", sheet_title)
        return

    # If sheet exists but first row empty → set headers
    rng = f"{sheet_title}!1:1"
    resp = svc.values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
    if not resp.get("values"):
        svc.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=rng,
            valueInputOption="RAW",
            body={"values": [list(headers)]},
        ).execute()
        log.info("gsheets: headers ensured on existing sheet '%s'", sheet_title)

def append_rows(sheet_title: str, rows: List[Sequence], retries: int = 3):
    svc = _svc().spreadsheets()
    rng = f"{sheet_title}!A1"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            res = svc.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=rng,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": rows},
            ).execute()
            upd = res.get("updates", {})
            log.info("gsheets: appended %s rows to '%s' (updatedRange=%s)",
                     len(rows), sheet_title, upd.get("updatedRange"))
            return res
        except HttpError as e:
            last_err = e
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            log.warning("gsheets: append attempt %d failed (status=%s): %s", attempt, status, e)
            if status in (429, 500, 503) and attempt < retries:
                time.sleep(0.8 * attempt)
                continue
            break
        except Exception as e:
            last_err = e
            log.exception("gsheets: append attempt %d failed with unexpected error", attempt)
            break
    # Bubble up the final error so we see it in app logs
    raise last_err if last_err else RuntimeError("append_rows failed for unknown reasons")
