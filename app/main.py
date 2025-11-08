# app/main.py
import os
import logging
import asyncio
import re
from pathlib import Path
from datetime import datetime, time as dtime, timezone

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import desc

from app.storage.db import Base, engine, SessionLocal
from app.storage.models import Contact, Thread, Message

# Providers
from app.providers.base import SmsProvider  # noop/dry-run provider
from app.providers import infobip as infobip_mod

# Routers
from app.routers.admin import router as admin_router
from app.routers.chat import router as chat_router

# LLM
from app.services.llm import summarize_thread_kpis, analyze
from app.services.llm import generate_reply_lt, classify_thread_outcome

# Google Sheets (existing tabs)
from app.services.gsheets import ensure_headers, append_rows

# Simple single-sheet lead log
from app.services.leadlog import init_leads_sheet, write_lead_row


# -----------------------------------------------------------------------------
# App + DB
# -----------------------------------------------------------------------------
app = FastAPI(title="SMS Bot")

# Static
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Routers
app.include_router(admin_router)
app.include_router(chat_router)

# Root → /admin for convenience
@app.get("/")
def _root():
    return RedirectResponse("/admin")

try:
    # Load .env from project root if present (no effect on server)
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

# Expose env flags for templates
app.state.env = os.getenv("APP_ENV", "local")
app.state.dry_run = os.getenv("DRY_RUN", "1") in ("1", "true", "True", 1, True)

Base.metadata.create_all(bind=engine)


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
PER_PERSON_MIN_SECONDS = int(os.getenv("PER_PERSON_MIN_SECONDS", "90"))
SKIP_BH = os.getenv("SKIP_BUSINESS_HOURS", "0") == "1"


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("sms-bot")


# -----------------------------------------------------------------------------
# Provider selection
# -----------------------------------------------------------------------------
if getattr(infobip_mod, "is_enabled", lambda: False)():
    provider = infobip_mod.InfobipProvider(dry_run=DRY_RUN)
    logger.info("Provider: Infobip (dry_run=%s)", DRY_RUN)
else:
    provider = SmsProvider(dry_run=True)  # base provider is always dry-run
    logger.warning("Provider: DRY-RUN base provider (Infobip not configured)")


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.head("/healthz")
def healthz_head():
    return {}

@app.get("/health")
def health():
    return JSONResponse({"ok": True})


# -----------------------------------------------------------------------------
# Google Sheets: ensure tabs on startup
# -----------------------------------------------------------------------------
@app.on_event("startup")
def _init_gsheets():
    try:
        ensure_headers("Outreach", ["ts_iso","match_id","phone","city","specialty","sms_text","result_ok","error","userref"])
        ensure_headers("Threads",  ["ts_iso","thread_id","phone","opener_city","opener_specialty","created_by"])
        ensure_headers("Messages", ["ts_iso","thread_id","phone","dir","text","model","prompt_sha"])
        ensure_headers("Failures", ["ts_iso","where","context","error"])
        ensure_headers("Status",   ["key","owner","stage","last_contacted","notes"])  # human-editable
        ensure_headers("OutreachEvents", ["ts_iso","event","thread_id","phone","city","specialty","text","userref","provider_id","latency_s"])
        ensure_headers("ThreadKPIs",    ["ts_iso","thread_id","phone","interested","future_interest","years"])

        # NEW: single “Leads” sheet for simple logging
        init_leads_sheet()

        logger.info("gsheets: headers ensured")
    except Exception as e:
        logger.warning("gsheets_init_failed: %s", e)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _latest_thread(db, phone: str) -> Thread | None:
    return (
        db.query(Thread)
          .filter(Thread.phone == phone)
          .order_by(Thread.id.desc())
          .first()
    )

def within_business_hours() -> bool:
    if SKIP_BH:
        return True
    now = datetime.now().time()
    return dtime(9, 0) <= now < dtime(18, 0)

def _ensure_contact_thread(db, phone: str):
    c = db.query(Contact).filter_by(phone=phone).first()
    if not c:
        c = Contact(phone=phone)
        db.add(c)
        db.flush()
    t = db.query(Thread).filter_by(phone=phone, status="open").first()
    if not t:
        t = Thread(phone=phone)
        db.add(t)
        db.flush()
    return c, t

def _append_outreach_event(*, event: str, thread_id: int, phone: str, text: str = "",
                           userref: str = "", provider_id: str = "", city: str = "", specialty: str = "",
                           latency_s: int | None = None):
    try:
        append_rows("OutreachEvents", [[
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            event, thread_id, phone, city, specialty, text, userref, provider_id,
            (latency_s if latency_s is not None else "")
        ]])
    except Exception:
        pass

def _finalize_thread_once(db, *, thread: Thread, phone: str, last_user_text: str = ""):
    # Already closed? bail.
    if getattr(thread, "status", "open") == "closed":
        return

    # 1) Compute & write KPIs WHILE THREAD IS OPEN (history is still visible)
    try:
        ensure_headers("ThreadKPIs", ["ts_iso","thread_id","phone","interested","future_interest","years"])
        k = summarize_thread_kpis(phone, last_user_text=last_user_text or "")
        append_rows("ThreadKPIs", [[
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            thread.id, phone, k["interested"], k["future_interest"], k["years"]
        ]])
    except Exception:
        pass

    # 2) Now close it once
    try:
        thread.status = "closed"
        db.add(thread)
        db.commit()
    except Exception:
        pass


# --- close detection (parity with chat.py/llm.py) ---
_POSTCLOSE_ACK_RE = re.compile(r"\b(ačiū|aciu|dėkui|dekui|ir\s+jums|geros\s+dienos|ok|okey|okei|thanks|👍)\b", re.I)

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[–—−-]", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\.!\s]+$", "", s)
    s = s.replace("ė","e").replace("ą","a").replace("č","c").replace("ę","e").replace("į","i").replace("š","s").replace("ų","u").replace("ū","u").replace("ž","z")
    return s

_CLOSE_PATTERNS = [
    re.compile(r"\bperduos(iu|iu)\s+koleg\w+.*\bpaskambins\b"),
    re.compile(r"\baci(u|u)\s+uz\s+(jusu\s+)?laika.*perduos(iu|iu)\s+koleg\w+"),
    re.compile(r"\bsupratau\s*-\s*daugiau\s+nerasysime\b"),
    re.compile(r"\buzsirasi(si(u|u)|au)\s+ateiciai\b"),
    re.compile(r"\bpalikime\s+cia\b"),
]

def _is_close_text(text: str) -> bool:
    last = _norm(text)
    if not last:
        return False
    if ("perduosiu koleg" in last and "paskambins" in last) or \
       ("daugiau nerasysime" in last) or \
       ("uzsirasi" in last and "ateiciai" in last) or \
       ("palikime cia" in last):
        return True
    return any(rx.search(last) for rx in _CLOSE_PATTERNS)

def _assistant_closed_history(db, thread_id: int) -> bool:
    last_out = (
        db.query(Message)
          .filter(Message.thread_id == thread_id, Message.dir == "out")
          .order_by(desc(Message.ts))
          .first()
    )
    return bool(last_out and _is_close_text(last_out.body or ""))


# -----------------------------------------------------------------------------
# Optional Infobip puller
# -----------------------------------------------------------------------------
from app.providers import infobip as _infobip
poll_log = logging.getLogger("poller")

INFOBIP_PULL = os.getenv("INFOBIP_PULL", "0") == "1"
INFOBIP_POLL_SECONDS = int(os.getenv("INFOBIP_POLL_SECONDS", "5"))

async def _process_inbound_item(item: dict):
    from_ = item.get("from") or ""
    text  = (item.get("text") or "").strip()
    text_l = text.lower()
    if not from_ or not text:
        return

    db = SessionLocal()

    # Early: if the latest thread is already closed by assistant and user sends a polite ack, finalize and stop.
    lt = _latest_thread(db, from_)
    if lt:
        try:
            if _assistant_closed_history(db, lt.id) and _POSTCLOSE_ACK_RE.search(text_l):
                _finalize_thread_once(db, thread=lt, phone=from_, last_user_text=text)
                return
        except Exception:
            pass

    try:
        c = db.query(Contact).filter_by(phone=from_).first()
        if not c:
            c = Contact(phone=from_)
            db.add(c)
        t = db.query(Thread).filter_by(phone=from_, status="open").first()
        if not t:
            t = Thread(phone=from_)
            db.add(t)
            db.flush()

        db.add(Message(thread_id=t.id, dir="in", body=text, status="delivered"))
        db.commit()

        # Keep Messages tab (best-effort)
        try:
            append_rows("Messages", [[
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                t.id, from_, "in", text, "", ""
            ]])
        except Exception:
            pass

        # First inbound → OutreachEvents reply_received + latency
        first_inbound = (
            db.query(Message)
            .filter(Message.thread_id == t.id, Message.dir == "in")
            .count()
        ) == 1

        if first_inbound:
            last_out = (
                db.query(Message)
                .filter(Message.thread_id == t.id, Message.dir == "out")
                .order_by(desc(Message.ts))
                .first()
            )
            latency_s = None
            if last_out:
                latency_s = int((datetime.utcnow() - last_out.ts).total_seconds())
            _append_outreach_event(
                event="reply_received", thread_id=t.id, phone=from_, text=text,
                userref="", provider_id="", city="", specialty="", latency_s=latency_s
            )

        # If already DNC, close and stop
        if c.dnc:
            _finalize_thread_once(db, thread=t, phone=from_, last_user_text=text)
            return

        # Thread-aware outcome
        msgs = (
            db.query(Message)
              .filter(Message.thread_id == t.id)
              .order_by(Message.ts.asc())
              .all()
        )
        history = [{"role": ("assistant" if m.dir == "out" else "user"), "content": m.body or ""} for m in msgs]

        outc = classify_thread_outcome(history)
        if outc.get("outcome") == "not_interested":
            c.dnc = True
            db.commit()
            # KPI write once on close
            _finalize_thread_once(db, thread=t, phone=from_, last_user_text=text)
            await provider.send(from_, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
            return

        reply  = generate_reply_lt({"msisdn": from_}, text)
        if reply:
            prov_id = await provider.send(from_, reply, userref="llm-reply")
            db.add(Message(thread_id=t.id, dir="out", body=reply, status="sent", provider_id=prov_id))
            db.commit()

            # OutreachEvents for LLM reply (poller path)
            try:
                _append_outreach_event(
                    event="llm_reply_sent",
                    thread_id=t.id,
                    phone=from_,
                    text=reply,
                    userref="llm-reply",
                    provider_id=(prov_id or ""),
                    city="",
                    specialty=""
                )
            except Exception:
                pass

            # One-sheet log for the assistant reply
            try:
                write_lead_row(
                    name="", phone=from_, city="", specialty="",
                    msg_dir="out", msg_text=reply, sent_ok=True, llm=None, note="poller_llm_reply"
                )
            except Exception:
                pass

            # Keep Messages tab (best-effort)
            try:
                append_rows("Messages", [[
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    t.id, from_, "out", reply, "", ""
                ]])
            except Exception:
                pass

            # If this assistant reply is a closing line → finalize once
            try:
                if _is_close_text(reply):
                    _finalize_thread_once(db, thread=t, phone=from_, last_user_text=text)
            except Exception:
                pass
    finally:
        db.close()

async def _infobip_poller():
    poll_log.info("Infobip puller started (every %ss)", INFOBIP_POLL_SECONDS)
    while True:
        try:
            items, _raw = _infobip.fetch_inbound(limit=100)
            if items:
                poll_log.info("Pulled %d MO", len(items))
                for it in items:
                    await _process_inbound_item(it)
        except Exception:
            poll_log.exception("Poller error")
        await asyncio.sleep(INFOBIP_POLL_SECONDS)

@app.on_event("startup")
async def _maybe_start_poller():
    if INFOBIP_PULL:
        asyncio.create_task(_infobip_poller())


# -----------------------------------------------------------------------------
# Outbound send (opener or manual) → log to Outreach + Leads
# -----------------------------------------------------------------------------
@app.post("/send")
async def send(payload: dict, force: bool = Query(False)):
    if not force and not within_business_hours():
        raise HTTPException(400, "Outside business hours (09:00–18:00)")

    to = payload["to"]
    body = payload.get("body") or payload.get("text")
    if not body:
        raise HTTPException(400, "Missing body/text")
    userref = payload.get("userref")

    db = SessionLocal()
    try:
        # DNC guard
        c = db.query(Contact).filter_by(phone=to).first()
        if c and c.dnc:
            raise HTTPException(403, "DNC/STOP on this contact")

        # Per-person throttle
        last_out = (
            db.query(Message)
              .join(Thread, Thread.id == Message.thread_id)
              .filter(Thread.phone == to, Message.dir == "out")
              .order_by(desc(Message.ts))
              .first()
        )
        if last_out and (datetime.utcnow() - last_out.ts).total_seconds() < PER_PERSON_MIN_SECONDS:
            raise HTTPException(429, "Per-person throttle")

        # Ensure contact/thread
        c, t = _ensure_contact_thread(db, to)

        # Send via selected provider
        prov_id = await provider.send(to, body, userref=userref)

        # Outreach event: opener vs manual
        _ev = "opener_sent" if (userref or "").lower().startswith("opener") else "outbound_sent"
        _append_outreach_event(
            event=_ev, thread_id=t.id, phone=to, text=body,
            userref=(userref or ""), provider_id=(prov_id or ""), city="", specialty=""
        )

        # Persist message
        m = Message(
            thread_id=t.id,
            dir="out",
            body=body,
            status="sent",
            provider_id=prov_id,
            userref=userref,
        )
        db.add(m)
        db.commit()

        # NEW: one-sheet log for manual/outbound send
        try:
            write_lead_row(
                name="", phone=to, city="", specialty="",
                msg_dir="out", msg_text=body, sent_ok=True, llm=None, note="manual_send"
            )
        except Exception:
            pass

        # Existing Outreach tab (best-effort)
        try:
            append_rows("Outreach", [[
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                None,
                to,
                "",
                "",
                body,
                True,
                "",
                userref or ""
            ]])
        except Exception:
            pass

        return {"id": prov_id}
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Inbound webhook (MO) → log to Messages + Leads, write KPIs ONLY on close
# -----------------------------------------------------------------------------
@app.post("/webhooks/mo")
async def mo(req: Request):
    payload = await req.json()
    mo = provider.parse_mo(payload, req.headers)

    msisdn = mo.get("from") or mo.get("msisdn") or ""
    text = (mo.get("text") or "").strip()
    text_l = text.lower()

    if not msisdn:
        raise HTTPException(400, "Missing sender")

    db = SessionLocal()

    # Early: if the latest thread is already closed by assistant and user sends a polite ack, finalize and stop.
    lt = _latest_thread(db, msisdn)
    if lt:
        try:
            if _assistant_closed_history(db, lt.id) and _POSTCLOSE_ACK_RE.search(text_l):
                _finalize_thread_once(db, thread=lt, phone=msisdn, last_user_text=text)
                return {"ok": True, "ack_after_close": True}
        except Exception:
            pass

    try:
        # Ensure contact/thread and store inbound
        c, t = _ensure_contact_thread(db, msisdn)
        db.add(Message(thread_id=t.id, dir="in", body=text, status="delivered"))
        db.commit()

        # First inbound → OutreachEvents reply_received + latency
        first_inbound = (
            db.query(Message)
            .filter(Message.thread_id == t.id, Message.dir == "in")
            .count()
        ) == 1

        if first_inbound:
            last_out = (
                db.query(Message)
                .filter(Message.thread_id == t.id, Message.dir == "out")
                .order_by(desc(Message.ts))
                .first()
            )
            latency_s = None
            if last_out:
                latency_s = int((datetime.utcnow() - last_out.ts).total_seconds())
            _append_outreach_event(
                event="reply_received", thread_id=t.id, phone=msisdn, text=text,
                userref="", provider_id="", city="", specialty="", latency_s=latency_s
            )

        # One-sheet log for inbound (classified)
        try:
            plan = analyze(text, [])
            write_lead_row(
                name="", phone=msisdn, city="", specialty="",
                msg_dir="in", msg_text=text, sent_ok=None, llm=plan, note="webhook_inbound"
            )
        except Exception:
            pass

        # Keep Messages tab (best-effort)
        try:
            append_rows("Messages", [[
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                t.id, msisdn, "in", text, "", ""
            ]])
        except Exception:
            pass

        # If already DNC, close and stop
        if c.dnc:
            logger.info("MO ignored (DNC) from %s: %r", msisdn, text)
            _finalize_thread_once(db, thread=t, phone=msisdn, last_user_text=text)
            return {"ok": True, "ignored": "dnc"}

        # Env-based DNC keywords
        raw = os.getenv("DNC_PHRASES", "")
        dnc_list = [p.strip().lower() for p in raw.split(",") if p.strip()]
        if dnc_list:
            norm = text_l.replace(",", " ").replace(".", " ")
            if any(p in norm for p in dnc_list):
                c.dnc = True
                db.commit()
                _finalize_thread_once(db, thread=t, phone=msisdn, last_user_text=text)
                await provider.send(msisdn, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
                return {"ok": True, "dnc": True}

        # Thread-aware outcome
        msgs = (
            db.query(Message)
              .filter(Message.thread_id == t.id)
              .order_by(Message.ts.asc())
              .all()
        )
        history = [{"role": ("assistant" if m.dir == "out" else "user"), "content": m.body or ""} for m in msgs]

        outc = classify_thread_outcome(history)
        if outc.get("outcome") == "not_interested":
            c.dnc = True
            db.commit()
            _finalize_thread_once(db, thread=t, phone=msisdn, last_user_text=text)
            await provider.send(msisdn, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
            return {
                "ok": True,
                "outcome": outc.get("outcome"),
                "future_maybe": outc.get("future_maybe"),
                "dnc": True,
            }

        # LLM reply (unchanged flow)
        reply = generate_reply_lt({"msisdn": msisdn}, text)

        if reply:
            prov_id = await provider.send(msisdn, reply, userref="llm-reply")
            db.add(Message(thread_id=t.id, dir="out", body=reply, status="sent", provider_id=prov_id))
            db.commit()

            # OutreachEvents for LLM reply (webhook path)
            try:
                _append_outreach_event(
                    event="llm_reply_sent",
                    thread_id=t.id,
                    phone=msisdn,
                    text=reply,
                    userref="llm-reply",
                    provider_id=(prov_id or ""),
                    city="",
                    specialty=""
                )
            except Exception:
                pass

            # One-sheet log for assistant reply
            try:
                write_lead_row(
                    name="", phone=msisdn, city="", specialty="",
                    msg_dir="out", msg_text=reply, sent_ok=True, llm=None, note="webhook_llm_reply"
                )
            except Exception:
                pass

            # Keep Messages tab (best-effort)
            try:
                append_rows("Messages", [[
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    t.id, msisdn, "out", reply, "", ""
                ]])
            except Exception:
                pass

            # If assistant reply itself is a closing line → finalize once
            try:
                if _is_close_text(reply):
                    _finalize_thread_once(db, thread=t, phone=msisdn, last_user_text=text)
            except Exception:
                pass

        return {
            "ok": True,
            "outcome": outc.get("outcome"),
            "future_maybe": outc.get("future_maybe"),
            "reply": reply
        }
    finally:
        db.close()


# -----------------------------------------------------------------------------
# Batch sender (reuses /send)
# -----------------------------------------------------------------------------
@app.post("/send-batch")
async def send_batch(payload: dict, force: bool = Query(False)):
    """
    payload: { "items": [ { "to": "...", "body": "...", "userref": "..." }, ... ] }
    """
    results = []
    for item in payload.get("items", []):
        try:
            r = await send(item, force=force)
            results.append({"to": item["to"], "ok": True, "id": r["id"]})
        except HTTPException as e:
            results.append({"to": item.get("to"), "ok": False, "error": e.detail})
        except Exception as e:
            results.append({"to": item.get("to"), "ok": False, "error": str(e)})
    return {"results": results}
