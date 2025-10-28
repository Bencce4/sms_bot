import os
import logging
from datetime import datetime, time as dtime

from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import desc

from app.storage.db import Base, engine, SessionLocal
from app.storage.models import Contact, Thread, Message

# Providers
from app.providers.base import SmsProvider  # noop/dry-run provider
from app.providers import infobip as infobip_mod

# LLM
from app.services.llm import classify_lt, generate_reply_lt

# -----------------------------------------------------------------------------
# App + DB
# -----------------------------------------------------------------------------
app = FastAPI(title="sms-bot")
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
#   If Infobip envs are present, use InfobipProvider; else use dry-run base.
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

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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


import asyncio
import logging
from app.providers import infobip as _infobip

log = logging.getLogger("poller")

INFOBIP_PULL = os.getenv("INFOBIP_PULL", "0") == "1"
INFOBIP_POLL_SECONDS = int(os.getenv("INFOBIP_POLL_SECONDS", "5"))

async def _process_inbound_item(item: dict):
    """
    Adapts pulled MO into the same flow as webhook.
    """
    from_ = item.get("from") or ""
    text  = (item.get("text") or "").strip()
    if not from_ or not text:
        return

    # Reuse your /webhooks/mo core by calling classify_and_reply_lt path directly
    from app.storage.db import SessionLocal
    from app.storage.models import Contact, Thread, Message
    from app.services.llm import classify_and_reply_lt, INTENT_STOP_SET

    db = SessionLocal()
    try:
        c = db.query(Contact).filter_by(phone=from_).first()
        if not c:
            c = Contact(phone=from_); db.add(c)
        t = db.query(Thread).filter_by(phone=from_, status="open").first()
        if not t:
            t = Thread(phone=from_); db.add(t); db.flush()

        db.add(Message(thread_id=t.id, dir="in", body=text, status="delivered"))
        db.commit()

        if c.dnc:
            return

        res = classify_and_reply_lt(from_, text)
        intent = (res.get("intent") or "").lower()
        reply  = (res.get("reply") or "").strip()

        if intent in INTENT_STOP_SET:
            c.dnc = True; db.commit()
            await provider.send(from_, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
            return

        if reply:
            await provider.send(from_, reply, userref="llm-reply")
            db.add(Message(thread_id=t.id, dir="out", body=reply, status="sent"))
            db.commit()
    finally:
        db.close()

async def _infobip_poller():
    log.info("Infobip puller started (every %ss)", INFOBIP_POLL_SECONDS)
    while True:
        try:
            items, raw = _infobip.fetch_inbound(limit=100)
            if items:
                log.info("Pulled %d MO", len(items))
                for it in items:
                    await _process_inbound_item(it)
        except Exception as e:
            log.exception("Poller error")
        await asyncio.sleep(INFOBIP_POLL_SECONDS)

@app.on_event("startup")
async def _maybe_start_poller():
    if INFOBIP_PULL:
        asyncio.create_task(_infobip_poller())


# -----------------------------------------------------------------------------
# Outbound send (opener or manual)
# -----------------------------------------------------------------------------
@app.post("/send")
async def send(payload: dict):
    if not within_business_hours():
        raise HTTPException(400, "Outside business hours (09:00–18:00)")

    to = payload["to"]
    body = payload["body"]
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
        return {"id": prov_id}
    finally:
        db.close()

# -----------------------------------------------------------------------------
# Inbound webhook (MO)
#   Provider.parse_mo → {from, text, ...}
#   DNC keywords → set DNC and confirm
#   Else LLM classify; stop -> DNC, else generate reply and send
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
    try:
        # Ensure contact/thread and store inbound
        c, t = _ensure_contact_thread(db, msisdn)
        db.add(Message(thread_id=t.id, dir="in", body=text, status="delivered"))
        db.commit()

        # If already DNC, do nothing
        if c.dnc:
            logger.info("MO ignored (DNC) from %s: %r", msisdn, text)
            return {"ok": True, "ignored": "dnc"}

        # Env-based DNC keywords (optional)
        raw = os.getenv("DNC_PHRASES", "")
        dnc_phrases = {p.strip().lower() for p in raw.split(",") if p.strip()}
        if dnc_phrases and text_l in dnc_phrases:
            c.dnc = True
            db.commit()
            await provider.send(msisdn, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
            return {"ok": True, "dnc": True}

        # LLM classify
        cls = classify_lt(text)  # {"intent": str, "confidence": float}
        intent = (cls.get("intent") or "").lower()

        if intent in {"stop", "not_interested", "do_not_contact", "unsubscribe"}:
            c.dnc = True
            db.commit()
            await provider.send(msisdn, "Supratau – daugiau netrukdysime. Gražios dienos!", userref="dnc-goodbye")
            return {"ok": True, "intent": intent, "confidence": cls.get("confidence"), "dnc": True}

        # LLM reply
        reply = generate_reply_lt({"msisdn": msisdn}, text)

        if reply:
            prov_id = await provider.send(msisdn, reply, userref="llm-reply")
            db.add(Message(thread_id=t.id, dir="out", body=reply, status="sent", provider_id=prov_id))
            db.commit()

        return {"ok": True, "intent": intent, "confidence": cls.get("confidence"), "reply": reply}
    finally:
        db.close()

# -----------------------------------------------------------------------------
# Batch sender (reuses /send)
# -----------------------------------------------------------------------------
@app.post("/send-batch")
async def send_batch(payload: dict):
    """
    payload: { "items": [ { "to": "...", "body": "...", "userref": "..." }, ... ] }
    """
    results = []
    for item in payload.get("items", []):
        try:
            r = await send(item)
            results.append({"to": item["to"], "ok": True, "id": r["id"]})
        except HTTPException as e:
            results.append({"to": item.get("to"), "ok": False, "error": e.detail})
        except Exception as e:
            results.append({"to": item.get("to"), "ok": False, "error": str(e)})
    return {"results": results}
