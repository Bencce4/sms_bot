import io
from fastapi import APIRouter, Request, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd

from app.services.storage import save_upload, latest_excel_path, load_sheets, save_matches_df, load_matches
from app.services.matcher import build_matches
from app.senders.infobip_client import send_sms
from app.util.logger import get_logger

log = get_logger("admin")
router = APIRouter()
from pathlib import Path
templates = Jinja2Templates(directory=str((Path(__file__).resolve().parent.parent)/"templates"))
templates = Jinja2Templates(directory=str((Path(__file__).resolve().parent.parent)/"templates"))

@router.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    last = latest_excel_path()
    have_matches = len(load_matches()) > 0
    return templates.TemplateResponse("upload.html", {"request": request, "last_excel": last, "have_matches": have_matches})

@router.post("/admin/upload")
async def admin_upload(file: UploadFile):
    data = await file.read()
    path = save_upload(data, file.filename)
    log.info({"event":"upload_saved","path":path})
    return RedirectResponse(url="/admin/parse", status_code=303)

@router.get("/admin/parse", response_class=HTMLResponse)
def admin_parse(request: Request):
    path = latest_excel_path()
    if not path:
        return RedirectResponse(url="/admin", status_code=303)
    people, projects = load_sheets(path)
    matches_df = build_matches(people, projects)
    save_matches_df(matches_df)
    cities = sorted({m.get("Miestas","") for m in matches_df.to_dict(orient="records")})
    profs = sorted({m.get("Specialybė","") for m in matches_df.to_dict(orient="records")})
    return templates.TemplateResponse("matches.html", {
        "request": request,
        "total": len(matches_df),
        "cities": cities,
        "profs": profs,
        "rows": matches_df.head(100).to_dict(orient="records"),  # preview first 100
        "limit": 20
    })

@router.post("/admin/preview", response_class=HTMLResponse)
async def admin_preview(request: Request, city: str = Form(""), prof: str = Form(""), limit: int = Form(20)):
    matches = load_matches()
    filt = [m for m in matches if (city=="" or m.get("Miestas","")==city) and (prof=="" or m.get("Specialybė","")==prof)]
    preview = filt[:max(0, int(limit))]
    # Render the same matches template with filtered preview
    cities = sorted({m.get("Miestas","") for m in matches})
    profs = sorted({m.get("Specialybė","") for m in matches})
    return templates.TemplateResponse("matches.html", {
        "request": request,
        "total": len(filt),
        "cities": cities,
        "profs": profs,
        "rows": preview,
        "limit": limit,
        "city_sel": city,
        "prof_sel": prof,
    })

@router.post("/admin/send")
async def admin_send(city: str = Form(""), prof: str = Form(""), limit: int = Form(20)):
    matches = load_matches()
    filt = [m for m in matches if (city=="" or m.get("Miestas","")==city) and (prof=="" or m.get("Specialybė","")==prof)]
    batch = filt[:max(0, int(limit))]

    results = []
    for m in batch:
        to = str(m.get("Tel. nr","")).strip()
        text = m.get("sms_text","")
        if not to or not text:
            results.append({"match_id": m.get("match_id"), "ok": False, "err": "missing to/text"})
            continue
        try:
            resp = send_sms(to, text, None)
            results.append({"match_id": m.get("match_id"), "ok": True, "resp": resp})
        except Exception as e:
            results.append({"match_id": m.get("match_id"), "ok": False, "err": str(e)})

    # simple result page
    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    html = "<h2>Send results</h2>"
    html += f"<p>OK: {ok} — Failed: {fail}</p>"
    html += '<p><a href="/admin">Back</a></p>'
    return HTMLResponse(html)
