# app/services/leadlog.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from app.services.gsheets import ensure_headers, append_rows

LEADS_SHEET = "Leads"

LEADS_HEADERS = [
    "ts_iso","name","phone","city","specialty",
    "msg_dir","msg_text","msg_sent",
    "outcome","job_interest","future_interest","intent",
    "years","availability_text",
    "model","prompt_sha","thread_id","note",
]

def init_leads_sheet():
    ensure_headers(LEADS_SHEET, LEADS_HEADERS)

def _val(d: Optional[Dict[str, Any]], k: str, default: Any = "") -> Any:
    if not d:
        return default
    v = d.get(k, default)
    # normalize python True/False/None into sheet-friendly simple values
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return v if v is not None else ""

def write_lead_row(
    *,
    name: str,
    phone: str,
    city: str,
    specialty: str,
    msg_dir: str,                 # "in" | "out"
    msg_text: str,
    sent_ok: Optional[bool],      # True (real SMS), False (prepared/simulated), None (received)
    llm: Optional[Dict[str, Any]],# analyzer plan or minimal dict
    note: str = "",
    model: str = "",
    prompt_sha: str = "",
    thread_id: Optional[int] = None,
):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    row = [
        ts, name or "", phone or "", city or "", specialty or "",
        msg_dir or "", msg_text or "",
        "" if sent_ok is None else ("TRUE" if sent_ok else "FALSE"),
        # LLM reductions (accept both analyze() output and your deterministic classify output)
        _val(llm, "outcome", ""),
        _val(llm, "job_interest", ""),
        _val(llm, "future_interest", ""),
        _val(llm, "intent", ""),
        _val(_val(llm, "slots", {}), "years", ""),
        _val(_val(llm, "slots", {}), "availability_text", ""),
        model or "", prompt_sha or "",
        (thread_id if thread_id is not None else ""),
        note or "",
    ]
    append_rows(LEADS_SHEET, [row])
