# app/routers/admin.py
from pathlib import Path
import io
import pandas as pd
from fastapi import APIRouter, Request, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.services.storage import (
    save_upload, latest_excel_path, load_sheets,
    save_matches_df, load_matches
)
from app.services.matcher import build_matches
from app.senders.infobip_client import send_sms
from app.util.logger import get_logger

log = get_logger("admin")
router = APIRouter()

# Absolute path to app/templates
TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

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

    # safe save (dedup columns etc.) – see storage.py hardening
    save_matches_df(matches_df)

    records = load_matches()  # already JSON-safe list[dict]
    cities = sorted({m.get("Miestas", "") for m in records})
    profs  = sorted({m.get("Specialybė", "") for m in records})

    people, projects = load_sheets(path)

    # log shapes + columns to help debugging
    import json
    from app.util.logger import get_logger
    log = get_logger("admin-parse")
    log.info({
    "event": "parse_loaded",
    "people_rows": len(people),
    "people_cols": list(map(str, people.columns)),
    "projects_rows": (len(projects) if projects is not None else None),
    "projects_cols": (list(map(str, projects.columns)) if projects is not None else None),
    })

    matches_df = build_matches(people, projects)

    log.info({
    "event": "matches_built",
    "rows": len(matches_df),
    "cols": list(map(str, matches_df.columns)),
    })


    return templates.TemplateResponse(
        "matches.html",
        {
            "request": request,
            "total": len(records),
            "cities": cities,
            "profs": profs,
            "rows": records[:100],  # preview first 100
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
            
log = __import__("logging").getLogger("admin-send");
p = request.app.state.provider;
log.info("ADMIN provider: dry_run=%s enabled=%s base=%r sender=%r key_len=%d", p.dry_run, p.is_enabled(), getattr(p,"api_base",None), getattr(p,"sender",None), len(getattr(p,"api_key","")));

log = __import__("logging").getLogger("admin-send");
p = request.app.state.provider;
log.info("ADMIN provider: dry_run=%s enabled=%s base=%r sender=%r key_len=%d", p.dry_run, p.is_enabled(), getattr(p,"api_base",None), getattr(p,"sender",None), len(getattr(p,"api_key","")));

log = __import__("logging").getLogger("admin-send");
p = request.app.state.provider;
log.info("ADMIN provider: dry_run=%s enabled=%s base=%r sender=%r key_len=%d", p.dry_run, p.is_enabled(), getattr(p,"api_base",None), getattr(p,"sender",None), len(getattr(p,"api_key","")));
print("ADMIN_SEND using app.state.provider:", type(request.app.state.provider).__name__, "dry_run=", getattr(request.app.state.provider,"dry_run",None)); pid = await request.app.state.provider.send(to, text)
            results.append({"match_id": m.get("match_id"), "ok": True, "resp": pid})
        except Exception as e:
            results.append({"match_id": m.get("match_id"), "ok": False, "err": str(e)})

    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    return templates.TemplateResponse(
        "send_results.html",
        {"request": request, "results": results, "ok": ok, "fail": fail},
    )
