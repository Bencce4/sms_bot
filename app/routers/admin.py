# app/routers/admin.py
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Request, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.storage.db import SessionLocal
from app.storage.models import Thread, Message
from app.services.llm import project_opener, analyze, PROMPT_SHA, MODEL as LLM_MODEL
from app.services.storage import (
    save_upload, latest_excel_path, load_sheets,
    save_matches_df, load_matches
)
from app.services.matcher import build_matches
from app.senders.infobip_client import send_sms
from app.util.logger import get_logger

# Sheets (existing tabs)
from datetime import datetime, timezone
from app.services.gsheets import append_rows, ensure_headers

# One-sheet lead log
from app.services.leadlog import write_lead_row

log = get_logger("admin")
router = APIRouter()

# templates
TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# --------------------- helpers ---------------------
def _get_or_create_thread(db: Session, phone: str) -> Thread:
    t = db.query(Thread).filter_by(phone=phone, status="open").first()
    if t:
        return t
    t = Thread(phone=phone, status="open")
    db.add(t)
    db.commit()         # ensure PK is assigned
    db.refresh(t)
    return t


# --------------------- open_chats (bulk open) ---------------------
@router.get("/admin/open_chats")
def open_chats_get_redirect():
    return RedirectResponse(url="/admin", status_code=303)

@router.post("/admin/open_chats", response_class=HTMLResponse)
async def admin_open_chats(
    request: Request,
    selected: Optional[List[str]] = Form(default=None),
):
    matches = load_matches()
    by_phone = {}
    for m in matches:
        phone = str(m.get("Tel. nr", "")).strip()
        if phone:
            by_phone[phone] = m

    phones = selected or []
    selected_rows = [by_phone[p] for p in phones if p in by_phone]
    if not selected_rows:
        html = [
            '<link rel="stylesheet" href="/static/styles.css">',
            "<h1>SMS Admin</h1>",
            "<h2>Open chats</h2>",
            "<p>No rows selected.</p>",
            '<p><a href="/admin">Back</a></p>',
        ]
        return HTMLResponse("\n".join(html))

    # Ensure headers once (best-effort)
    try:
        ensure_headers("Threads",  ["ts_iso","thread_id","phone","opener_city","opener_specialty","created_by"])
        ensure_headers("Messages", ["ts_iso","thread_id","phone","dir","text","model","prompt_sha"])
    except Exception as e:
        log.warning("gsheets_ensure_headers_failed err=%s", e)

    links = []
    db = SessionLocal()
    try:
        for row in selected_rows:
            phone = str(row.get("Tel. nr", "")).strip()
            if not phone:
                continue

            name = (row.get("Vardas") or row.get("Name") or "Sveiki")
            city = (row.get("Miestas") or "Lietuvoje")
            spec = (row.get("Specialybė") or "statybos")

            t = _get_or_create_thread(db, phone)

            # Add opener only if thread has no messages yet
            has_msgs = db.query(Message).filter(Message.thread_id == t.id).first() is not None
            if not has_msgs:
                opener = project_opener(name=name, city=city, specialty=spec)
                db.add(Message(thread_id=t.id, dir="out", body=opener))
                db.commit()

                # --- Existing tabs logging (best-effort) ---
                try:
                    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    append_rows("Threads",  [[ts, t.id, phone, city, spec, "admin_open_chats"]])
                    append_rows("Messages", [[ts, t.id, phone, "out", opener, LLM_MODEL, PROMPT_SHA]])
                    log.info("gsheets_seed_logged phone=%s thread_id=%s", phone, t.id)
                except Exception as e:
                    log.warning("gsheets_seed_log_failed phone=%s err=%s", phone, e)

                # --- Leads log for the opener (seeded, NOT actually sent) ---
                try:
                    llm = analyze(opener, [])
                    write_lead_row(
                        name=name or "",
                        phone=phone,
                        city=city or "",
                        specialty=spec or "",
                        msg_dir="out",
                        msg_text=opener,
                        sent_ok=False,                  # <- seeded message, not a real SMS send
                        llm=llm,
                        note="admin_open_chats_seed",
                        model=LLM_MODEL,
                        prompt_sha=PROMPT_SHA,
                        thread_id=t.id,
                    )
                except Exception as e:
                    log.warning("leads_log_failed phone=%s err=%s", phone, e)

            links.append(f"/admin/chat?phone={phone}")
    finally:
        db.close()

    html = [
        '<link rel="stylesheet" href="/static/styles.css">',
        "<h1>SMS Admin</h1>",
        "<h2>Open chats</h2>",
        f"<p>Prepared {len(links)} chats.</p>",
        "<ul>",
        *[f'<li><a target="_blank" href="{u}">{u}</a></li>' for u in links],
        "</ul>",
        '<p><a href="/admin">Back</a></p>',
    ]
    return HTMLResponse("\n".join(html))


# --------------------- existing admin routes ---------------------
@router.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    last = latest_excel_path()
    have_matches = len(load_matches()) > 0
    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "last_excel": last,
            "have_matches": have_matches,
            "env": request.app.state.env,
            "dry_run": request.app.state.dry_run,
        },
    )

@router.post("/admin/upload")
async def admin_upload(file: UploadFile):
    data = await file.read()
    path = save_upload(data, file.filename)
    log.info({"event": "upload_saved", "path": path})
    return RedirectResponse(url="/admin/parse", status_code=303)

@router.get("/admin/parse", response_class=HTMLResponse)
def admin_parse(request: Request):
    path = latest_excel_path()
    if not path:
        return RedirectResponse(url="/admin", status_code=303)

    people, projects = load_sheets(path)
    matches_df = build_matches(people, projects)
    save_matches_df(matches_df)

    records = load_matches()
    cities = sorted({m.get("Miestas", "") for m in records})
    profs  = sorted({m.get("Specialybė", "") for m in records})

    log_info = {
        "event": "parse_loaded",
        "people_rows": len(people),
        "people_cols": list(map(str, people.columns)),
        "projects_rows": (len(projects) if projects is not None else None),
        "projects_cols": (list(map(str, projects.columns)) if projects is not None else None),
    }
    log.info(log_info)
    log.info({"event": "matches_built", "rows": len(matches_df), "cols": list(map(str, matches_df.columns))})

    return templates.TemplateResponse(
        "matches.html",
        {
            "request": request,
            "total": len(records),
            "cities": cities,
            "profs": profs,
            "rows": records[:100],
            "limit": 20,
        },
    )

@router.post("/admin/preview", response_class=HTMLResponse)
async def admin_preview(request: Request, city: str = Form(""), prof: str = Form(""), limit: int = Form(20)):
    matches = load_matches()
    filt = [
        m for m in matches
        if (not city or m.get("Miestas", "") == city)
        and (not prof or m.get("Specialybė", "") == prof)
    ]
    cities = sorted({m.get("Miestas", "") for m in matches})
    profs  = sorted({m.get("Specialybė", "") for m in matches})

    return templates.TemplateResponse(
        "matches.html",
        {
            "request": request,
            "total": len(filt),
            "cities": cities,
            "profs": profs,
            "rows": filt[: max(0, int(limit))],
            "limit": limit,
            "city_sel": city,
            "prof_sel": prof,
        },
    )

@router.post("/admin/send", response_class=HTMLResponse)
async def admin_send(request: Request, city: str = Form(""), prof: str = Form(""), limit: int = Form(20)):
    matches = load_matches()
    batch = [
        m for m in matches
        if (not city or m.get("Miestas", "") == city)
        and (not prof or m.get("Specialybė", "") == prof)
    ][: max(0, int(limit))]

    results = []
    for m in batch:
        to = str(m.get("Tel. nr", "")).strip()
        text = (m.get("sms_text") or m.get("Text") or "").strip()
        if not to or not text:
            results.append({"match_id": m.get("match_id"), "ok": False, "err": "missing to/text"})
            continue
        try:
            resp = send_sms(to, text, None)

            # ---- Existing Outreach logging (best-effort) ----
            ensure_headers("Outreach", ["ts_iso","match_id","phone","city","specialty","sms_text","result_ok","error","userref"])
            ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
            row = [ts, m.get("match_id"), to, m.get("Miestas"), m.get("Specialybė"), text, True, "", None]
            append_rows("Outreach", [row])
            log.info("gsheets_outreach_row_written match_id=%s phone=%s", m.get("match_id"), to)

            # ---- Leads log for admin batch send (real SMS) ----
            try:
                llm = analyze(text, [])
                write_lead_row(
                    name=(m.get("Vardas") or m.get("Name") or ""),
                    phone=to,
                    city=(m.get("Miestas") or ""),
                    specialty=(m.get("Specialybė") or ""),
                    msg_dir="out",
                    msg_text=text,
                    sent_ok=True,
                    llm=llm,
                    note="admin_send_batch",
                    model=LLM_MODEL,
                    prompt_sha=PROMPT_SHA,
                    thread_id=None,
                )
            except Exception as e:
                log.warning("leads_log_failed phone=%s err=%s", to, e)
            # ----------------------------------------------------------

            results.append({"match_id": m.get("match_id"), "ok": True, "resp": resp})
        except Exception as e:
            log.warning("gsheets_outreach_row_failed match_id=%s phone=%s err=%s", m.get("match_id"), to, e)
            results.append({"match_id": m.get("match_id"), "ok": False, "err": str(e)})

    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    return templates.TemplateResponse(
        "send_results.html",
        {"request": request, "results": results, "ok": ok, "fail": fail},
    )
